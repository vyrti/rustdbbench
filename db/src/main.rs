// rust_redis_server/src/main.rs
use anyhow::Result;
use async_trait::async_trait;
use bytes::{Buf, Bytes, BytesMut};
use clap::Parser;
use rustc_hash::{FxHashMap, FxHasher};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex as StdMutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const IN_MEMORY_SHARDS: usize = 128;

//================================================================
// Command Line Interface
//================================================================

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = "A RESP-compliant, in-memory database server.")]
struct Cli {
    #[clap(short, long, default_value = "127.0.0.1:7878")]
    addr: String,
}

//================================================================
// In-Memory Key-Value Store (The "Engine")
//================================================================

#[async_trait]
pub trait KvStore: Send + Sync {
    async fn set(&self, key: Bytes, value: Bytes) -> Result<()>;
    async fn get(&self, key: &Bytes) -> Result<Option<Bytes>>;
    async fn lpush(&self, key: &Bytes, value: Bytes) -> Result<()>;
    async fn ltrim(&self, key: &Bytes, start: isize, stop: isize) -> Result<()>;
    async fn lrange(&self, key: &Bytes, start: isize, stop: isize) -> Result<Vec<Bytes>>;
}

type Shard = FxHashMap<Bytes, Vec<Bytes>>;

#[derive(Clone)]
pub struct InMemoryStore {
    shards: Arc<Vec<StdMutex<Shard>>>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        let mut shards = Vec::with_capacity(IN_MEMORY_SHARDS);
        for _ in 0..IN_MEMORY_SHARDS {
            shards.push(StdMutex::new(FxHashMap::default()));
        }
        Self { shards: Arc::new(shards) }
    }

    fn get_shard_index<K: Hash>(&self, key: &K) -> usize {
        let mut hasher = FxHasher::default();
        key.hash(&mut hasher);
        hasher.finish() as usize % IN_MEMORY_SHARDS
    }
}

#[async_trait]
impl KvStore for InMemoryStore {
    async fn set(&self, key: Bytes, value: Bytes) -> Result<()> {
        let index = self.get_shard_index(&key);
        let mut shard = self.shards[index].lock().unwrap();
        shard.insert(key, vec![value]);
        Ok(())
    }

    async fn get(&self, key: &Bytes) -> Result<Option<Bytes>> {
        let index = self.get_shard_index(key);
        let shard = self.shards[index].lock().unwrap();
        Ok(shard.get(key).and_then(|v| v.first().cloned()))
    }

    async fn lpush(&self, key: &Bytes, value: Bytes) -> Result<()> {
        let index = self.get_shard_index(key);
        let mut shard = self.shards[index].lock().unwrap();
        let list = shard.entry(key.clone()).or_default();
        list.insert(0, value);
        Ok(())
    }

    async fn ltrim(&self, key: &Bytes, start: isize, stop: isize) -> Result<()> {
        let index = self.get_shard_index(key);
        let mut shard = self.shards[index].lock().unwrap();
        if let Some(list) = shard.get_mut(key) {
            if start == 0 && stop >= 0 {
                list.truncate((stop + 1) as usize);
            }
        }
        Ok(())
    }

    async fn lrange(&self, key: &Bytes, start: isize, stop: isize) -> Result<Vec<Bytes>> {
        let index = self.get_shard_index(key);
        let shard = self.shards[index].lock().unwrap();
        if let Some(list) = shard.get(key) {
            if start == 0 && stop >= 0 {
                let end = (stop + 1).min(list.len() as isize) as usize;
                return Ok(list[..end].to_vec());
            }
        }
        Ok(vec![])
    }
}

//================================================================
// Server, Connection, and RESP Protocol Handling
//================================================================

struct Connection {
    stream: TcpStream,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Connection {
            stream,
            buffer: BytesMut::with_capacity(4096),
        }
    }

    async fn read_frame(&mut self) -> Result<Option<RespFrame>> {
        loop {
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }
            if self.stream.read_buf(&mut self.buffer).await? == 0 {
                return if self.buffer.is_empty() { Ok(None) } else { Err(anyhow::anyhow!("connection reset by peer")) };
            }
        }
    }

    fn parse_frame(&mut self) -> Result<Option<RespFrame>> {
        if self.buffer.is_empty() { return Ok(None); }
        match self.buffer[0] {
            b'*' => self.parse_array(),
            _ => Err(anyhow::anyhow!("invalid frame; expected array")),
        }
    }

    fn parse_array(&mut self) -> Result<Option<RespFrame>> {
        let mut parsable_view = &self.buffer[1..];
        
        if let Some((len, _)) = parse_decimal(&mut parsable_view)? {
            let mut elements = Vec::with_capacity(len as usize);
            for _ in 0..len {
                if parsable_view.is_empty() || parsable_view[0] != b'$' { return Ok(None); }
                parsable_view = &parsable_view[1..];
                
                if let Some((bulk_len, _)) = parse_decimal(&mut parsable_view)? {
                    let data_len = bulk_len as usize;
                    if parsable_view.len() < data_len + 2 { return Ok(None); }
                    let data = Bytes::copy_from_slice(&parsable_view[..data_len]);
                    elements.push(RespFrame::Bulk(data));
                    parsable_view = &parsable_view[data_len + 2..];
                } else { return Ok(None); }
            }
            
            let consumed = self.buffer.len() - parsable_view.len();
            self.buffer.advance(consumed);
            Ok(Some(RespFrame::Array(elements)))
        } else { Ok(None) }
    }
}

fn parse_decimal(buf: &mut &[u8]) -> Result<Option<(i64, usize)>> {
    if let Some(line_end) = buf.iter().position(|&b| b == b'\r') {
        // If the \n isn't in the buffer yet, we can't parse the full line.
        if line_end + 1 >= buf.len() {
            return Ok(None);
        }
        // If the character after \r is not \n, it's a protocol error.
        if buf[line_end + 1] != b'\n' {
            return Err(anyhow::anyhow!("malformed line ending"));
        }

        let num_slice = &buf[..line_end];
        let n = std::str::from_utf8(num_slice)?.parse::<i64>()?;
        *buf = &buf[line_end + 2..];
        Ok(Some((n, num_slice.len() + 2)))
    } else {
        Ok(None)
    }
}

#[derive(Debug)]
enum RespFrame {
    Array(Vec<RespFrame>),
    Bulk(Bytes),
}

#[derive(Debug)]
enum Command {
    Set(Bytes, Bytes),
    Get(Bytes),
    LPush(Bytes, Bytes),
    LTrim(Bytes, isize, isize),
    LRange(Bytes, isize, isize),
    Ping,
    Unknown(Bytes),
}

impl Command {
    fn from_frame(frame: RespFrame) -> Result<Command> {
        let mut array = match frame {
            RespFrame::Array(array) => array,
            _ => return Err(anyhow::anyhow!("command must be an array")),
        };
        let command_name = match array.remove(0) {
            RespFrame::Bulk(data) => data,
            _ => return Err(anyhow::anyhow!("command name must be a bulk string")),
        };

        let name = command_name.to_ascii_uppercase();
        let mut args = array.into_iter().map(|f| match f {
            RespFrame::Bulk(b) => b,
            _ => Bytes::new(),
        });

        match &name[..] {
            b"PING" => Ok(Command::Ping),
            b"SET" => Ok(Command::Set(args.next().unwrap(), args.next().unwrap())),
            b"GET" => Ok(Command::Get(args.next().unwrap())),
            b"LPUSH" => Ok(Command::LPush(args.next().unwrap(), args.next().unwrap())),
            b"LTRIM" => {
                let key = args.next().unwrap();
                let start = std::str::from_utf8(&args.next().unwrap())?.parse()?;
                let stop = std::str::from_utf8(&args.next().unwrap())?.parse()?;
                Ok(Command::LTrim(key, start, stop))
            }
            b"LRANGE" => {
                let key = args.next().unwrap();
                let start = std::str::from_utf8(&args.next().unwrap())?.parse()?;
                let stop = std::str::from_utf8(&args.next().unwrap())?.parse()?;
                Ok(Command::LRange(key, start, stop))
            }
            _ => Ok(Command::Unknown(command_name)),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let listener = TcpListener::bind(&cli.addr).await?;
    println!("RustRedisServer listening on {}", cli.addr);
    let db = Arc::new(InMemoryStore::new());

    loop {
        let (stream, addr) = listener.accept().await?;
        let db_clone = db.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, db_clone).await {
                 eprintln!("[{}] Connection error: {}", addr, e);
            }
        });
    }
}

async fn handle_connection(stream: TcpStream, db: Arc<InMemoryStore>) -> Result<()> {
    let mut conn = Connection::new(stream);
    loop {
        match conn.read_frame().await? {
            Some(frame) => {
                let cmd = Command::from_frame(frame)?;
                let response = match cmd {
                    Command::Ping => Bytes::from_static(b"+PONG\r\n"),
                    Command::Set(key, val) => {
                        db.set(key, val).await?;
                        Bytes::from_static(b"+OK\r\n")
                    }
                    Command::Get(key) => match db.get(&key).await? {
                        Some(val) => {
                            let mut resp = BytesMut::with_capacity(val.len() + 10);
                            resp.extend_from_slice(format!("${}\r\n", val.len()).as_bytes());
                            resp.extend_from_slice(&val);
                            resp.extend_from_slice(b"\r\n");
                            resp.freeze()
                        }
                        None => Bytes::from_static(b"$-1\r\n"),
                    },
                    Command::LPush(key, val) => {
                        db.lpush(&key, val).await?;
                        Bytes::from_static(b"+OK\r\n")
                    }
                    Command::LTrim(key, start, stop) => {
                        db.ltrim(&key, start, stop).await?;
                        Bytes::from_static(b"+OK\r\n")
                    }
                    Command::LRange(key, start, stop) => {
                        let items = db.lrange(&key, start, stop).await?;
                        let mut resp = BytesMut::new();
                        resp.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                        for item in items {
                            resp.extend_from_slice(format!("${}\r\n", item.len()).as_bytes());
                            resp.extend_from_slice(&item);
                            resp.extend_from_slice(b"\r\n");
                        }
                        resp.freeze()
                    }
                    Command::Unknown(cmd) => {
                        let err_msg = format!("-ERR unknown command `{:?}`\r\n", String::from_utf8_lossy(&cmd));
                        Bytes::from(err_msg)
                    }
                };
                conn.stream.write_all(&response).await?;
            }
            None => break,
        }
    }
    Ok(())
}