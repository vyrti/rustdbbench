// rust_redis_server/src/main.rs
use anyhow::Result;
use bytes::{Buf, Bytes, BytesMut};
use clap::Parser;
use futures::stream::{StreamExt};
use futures::SinkExt;
use smallvec::SmallVec;
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{Decoder, Encoder, Framed};
use tracing::{debug, error, info, instrument};
use rustc_hash::FxHashMap;

//================================================================
// Command Line Interface
//================================================================

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = "A RESP-compliant, in-memory database server.")]
struct Cli {
    #[clap(short, long, default_value = "127.0.0.1:7878")]
    addr: String,
    
    #[clap(short = 't', long, default_value_t = num_cpus::get())]
    threads: usize,
}

//================================================================
// In-Memory Key-Value Store (The "Engine") - SYNCHRONOUS
//================================================================

pub trait KvStore: Send + Sync {
    fn set(&self, key: Bytes, value: Bytes) -> Result<()>;
    fn get(&self, key: &Bytes) -> Result<Option<Bytes>>;
    fn lpush(&self, key: &Bytes, value: Bytes) -> Result<()>;
    fn ltrim(&self, key: &Bytes, start: isize, stop: isize) -> Result<()>;
    fn lrange(&self, key: &Bytes, start: isize, stop: isize) -> Result<SmallVec<[Bytes; 8]>>;
}

type ListValue = SmallVec<[Bytes; 8]>;
const IN_MEMORY_SHARDS: usize = 256; // Increased shards slightly

#[derive(Clone)]
pub struct InMemoryStore {
    shards: Arc<Vec<StdMutex<FxHashMap<Bytes, ListValue>>>>,
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
        let mut hasher = rustc_hash::FxHasher::default();
        key.hash(&mut hasher);
        hasher.finish() as usize % IN_MEMORY_SHARDS
    }
}

impl KvStore for InMemoryStore {
    #[inline]
    fn set(&self, key: Bytes, value: Bytes) -> Result<()> {
        let mut list = SmallVec::new();
        list.push(value);
        let index = self.get_shard_index(&key);
        let mut shard = self.shards[index].lock().unwrap();
        shard.insert(key, list);
        Ok(())
    }

    #[inline]
    fn get(&self, key: &Bytes) -> Result<Option<Bytes>> {
        let index = self.get_shard_index(key);
        let shard = self.shards[index].lock().unwrap();
        Ok(shard.get(key).and_then(|entry| entry.first().cloned()))
    }

    #[inline]
    fn lpush(&self, key: &Bytes, value: Bytes) -> Result<()> {
        let index = self.get_shard_index(key);
        let mut shard = self.shards[index].lock().unwrap();
        let list = shard.entry(key.clone()).or_default();
        list.insert(0, value);
        Ok(())
    }

    #[inline]
    fn ltrim(&self, key: &Bytes, start: isize, stop: isize) -> Result<()> {
        let index = self.get_shard_index(key);
        let mut shard = self.shards[index].lock().unwrap();
        if let Some(list) = shard.get_mut(key) {
            if start == 0 && stop >= 0 {
                let end = (stop + 1) as usize;
                if end < list.len() {
                    list.truncate(end);
                }
            }
        }
        Ok(())
    }

    #[inline]
    fn lrange(&self, key: &Bytes, start: isize, stop: isize) -> Result<SmallVec<[Bytes; 8]>> {
        let index = self.get_shard_index(key);
        let shard = self.shards[index].lock().unwrap();
        if let Some(list) = shard.get(key) {
            if start == 0 && stop >= 0 {
                let end = (stop + 1).min(list.len() as isize) as usize;
                return Ok(list[..end].iter().cloned().collect());
            }
        }
        Ok(SmallVec::new())
    }
}

//================================================================
// RESP Protocol Codec
//================================================================

pub struct RespCodec;

#[derive(Debug, Clone)]
pub enum RespFrame {
    Array(SmallVec<[Box<RespFrame>; 8]>),
    Bulk(Bytes),
    SimpleString(Bytes),
    Error(Bytes),
    Null,
}

impl Decoder for RespCodec {
    type Item = RespFrame;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let (frame, len) = match Self::parse_frame(src) {
            Ok(Some((frame, len))) => (frame, len),
            Ok(None) => return Ok(None),
            Err(e) => return Err(e),
        };
        src.advance(len);
        Ok(Some(frame))
    }
}

impl RespCodec {
    fn parse_frame(src: &[u8]) -> Result<Option<(RespFrame, usize)>> {
        if src.is_empty() { return Ok(None); }
        match src[0] {
            b'*' => Self::parse_array(src),
            b'$' => Self::parse_bulk_string(src),
            b'+' => Self::parse_simple_string(src),
            b'-' => Self::parse_error(src),
            _ => Err(anyhow::anyhow!("Invalid RESP frame type")),
        }
    }

    fn parse_array(src: &[u8]) -> Result<Option<(RespFrame, usize)>> {
        let (len, mut consumed) = match Self::parse_integer(&src[1..])? {
            Some((len, consumed)) => (len, consumed + 1),
            None => return Ok(None),
        };
        if len == -1 { return Ok(Some((RespFrame::Null, consumed))); }
        let len = len as usize;
        let mut elements = SmallVec::with_capacity(len);
        for _ in 0..len {
            match Self::parse_frame(&src[consumed..])? {
                Some((frame, frame_len)) => {
                    elements.push(Box::new(frame));
                    consumed += frame_len;
                }
                None => return Ok(None),
            }
        }
        Ok(Some((RespFrame::Array(elements), consumed)))
    }

    fn parse_bulk_string(src: &[u8]) -> Result<Option<(RespFrame, usize)>> {
        let (len, consumed) = match Self::parse_integer(&src[1..])? {
            Some((len, consumed)) => (len, consumed + 1),
            None => return Ok(None),
        };
        if len == -1 { return Ok(Some((RespFrame::Null, consumed))); }
        let len = len as usize;
        let total_len = consumed + len + 2;
        if src.len() < total_len { return Ok(None); }
        let data = Bytes::copy_from_slice(&src[consumed..consumed + len]);
        Ok(Some((RespFrame::Bulk(data), total_len)))
    }
    
    fn parse_simple_string(src: &[u8]) -> Result<Option<(RespFrame, usize)>> {
        if let Some(pos) = src[1..].windows(2).position(|w| w == b"\r\n") {
            let total_len = pos + 3;
            let data = Bytes::copy_from_slice(&src[1..pos+1]);
            Ok(Some((RespFrame::SimpleString(data), total_len)))
        } else { Ok(None) }
    }

    fn parse_error(src: &[u8]) -> Result<Option<(RespFrame, usize)>> {
        if let Some(pos) = src[1..].windows(2).position(|w| w == b"\r\n") {
            let total_len = pos + 3;
            let data = Bytes::copy_from_slice(&src[1..pos+1]);
            Ok(Some((RespFrame::Error(data), total_len)))
        } else { Ok(None) }
    }

    fn parse_integer(buf: &[u8]) -> Result<Option<(i64, usize)>> {
        if let Some(pos) = buf.windows(2).position(|w| w == b"\r\n") {
            let num_str = std::str::from_utf8(&buf[..pos])?;
            let num = num_str.parse::<i64>()?;
            Ok(Some((num, pos + 2)))
        } else { Ok(None) }
    }
}


impl Encoder<RespFrame> for RespCodec {
    type Error = anyhow::Error;
    fn encode(&mut self, item: RespFrame, dst: &mut BytesMut) -> Result<()> {
        match item {
            RespFrame::SimpleString(data) => {
                dst.extend_from_slice(b"+");
                dst.extend_from_slice(&data);
                dst.extend_from_slice(b"\r\n");
            }
            RespFrame::Error(data) => {
                dst.extend_from_slice(b"-");
                dst.extend_from_slice(&data);
                dst.extend_from_slice(b"\r\n");
            }
            RespFrame::Bulk(data) => {
                dst.extend_from_slice(format!("${}\r\n", data.len()).as_bytes());
                dst.extend_from_slice(&data);
                dst.extend_from_slice(b"\r\n");
            }
            RespFrame::Array(elements) => {
                dst.extend_from_slice(format!("*{}\r\n", elements.len()).as_bytes());
                for element in elements {
                    self.encode(*element, dst)?;
                }
            }
            RespFrame::Null => {
                dst.extend_from_slice(b"$-1\r\n");
            }
        }
        Ok(())
    }
}

//================================================================
// Command Processing
//================================================================

#[derive(Debug)]
enum Command {
    Set(Bytes, Bytes), Get(Bytes), LPush(Bytes, Bytes), LTrim(Bytes, isize, isize), LRange(Bytes, isize, isize), Ping, Unknown(Bytes),
}

fn next_bulk(args: &mut impl Iterator<Item = Box<RespFrame>>) -> Result<Bytes> {
    match args.next() {
        Some(fr) => match *fr {
            RespFrame::Bulk(b) => Ok(b),
            _ => Err(anyhow::anyhow!("argument must be a bulk string")),
        },
        None => Err(anyhow::anyhow!("not enough arguments")),
    }
}
fn next_parsable<T>(args: &mut impl Iterator<Item = Box<RespFrame>>) -> Result<T>
where T: FromStr, <T as FromStr>::Err: std::error::Error + Send + Sync + 'static {
    let bytes = next_bulk(args)?;
    Ok(std::str::from_utf8(&bytes)?.parse::<T>()?)
}

impl Command {
    #[inline]
    fn from_frame(frame: RespFrame) -> Result<Command> {
        let array = match frame {
            RespFrame::Array(array) => array,
            _ => return Err(anyhow::anyhow!("command must be an array")),
        };
        let mut args = array.into_iter();
        let command_name_frame = args.next().ok_or_else(|| anyhow::anyhow!("empty command"))?;
        let command_name = match *command_name_frame {
            RespFrame::Bulk(data) => data,
            _ => return Err(anyhow::anyhow!("command name must be a bulk string")),
        };
        let name = command_name.to_ascii_uppercase();
        let command = match &name[..] {
            b"PING" => Command::Ping,
            b"SET" => Command::Set(next_bulk(&mut args)?, next_bulk(&mut args)?),
            b"GET" => Command::Get(next_bulk(&mut args)?),
            b"LPUSH" => Command::LPush(next_bulk(&mut args)?, next_bulk(&mut args)?),
            b"LTRIM" => Command::LTrim(next_bulk(&mut args)?, next_parsable(&mut args)?, next_parsable(&mut args)?),
            b"LRANGE" => Command::LRange(next_bulk(&mut args)?, next_parsable(&mut args)?, next_parsable(&mut args)?),
            _ => Command::Unknown(command_name),
        };
        if args.next().is_some() { return Err(anyhow::anyhow!("wrong number of arguments for command")); }
        Ok(command)
    }
}

//================================================================
// Server and Connection Handling
//================================================================

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_thread_ids(true)
        .init();

    let cli = Cli::parse();
    
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cli.threads)
        .enable_all()
        .build()?;

    rt.block_on(async {
        let listener = TcpListener::bind(&cli.addr).await?;
        info!("RustRedisServer listening on {} with {} threads", cli.addr, cli.threads);
        
        let db = Arc::new(InMemoryStore::new());

        loop {
            let (stream, addr) = listener.accept().await?;
            let db_clone = db.clone();
            
            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, db_clone).await {
                    if !e.to_string().contains("Connection reset by peer") {
                        error!(client_addr = %addr, error = %e, "Connection error");
                    }
                }
            });
        }
    })
}

#[instrument(skip(stream, db), fields(client_addr = %stream.peer_addr().unwrap()))]
async fn handle_connection(stream: TcpStream, db: Arc<InMemoryStore>) -> Result<()> {
    stream.set_nodelay(true)?;
    
    let mut framed = Framed::new(stream, RespCodec);
    
    while let Some(frame_result) = framed.next().await {
        let response = match frame_result {
            Ok(frame) => {
                match Command::from_frame(frame) {
                    Ok(cmd) => execute_command(cmd, &db),
                    Err(e) => RespFrame::Error(Bytes::from(format!("ERR {}", e.to_string()))),
                }
            }
            Err(e) => return Err(e),
        };
        framed.send(response).await?;
    }
    
    Ok(())
}

// This function is synchronous and executes directly on the async worker's thread.
// This is fast because the work is trivial (nanoseconds) and avoids scheduling overhead.
#[inline]
fn execute_command(cmd: Command, db: &InMemoryStore) -> RespFrame {
    match cmd {
        Command::Ping => RespFrame::SimpleString(Bytes::from_static(b"PONG")),
        Command::Set(key, val) => {
            match db.set(key, val) {
                Ok(_) => RespFrame::SimpleString(Bytes::from_static(b"OK")),
                Err(e) => RespFrame::Error(Bytes::from(e.to_string())),
            }
        }
        Command::Get(key) => match db.get(&key) {
            Ok(Some(val)) => RespFrame::Bulk(val),
            Ok(None) => RespFrame::Null,
            Err(e) => RespFrame::Error(Bytes::from(e.to_string())),
        },
        Command::LPush(key, val) => {
            match db.lpush(&key, val) {
                Ok(_) => RespFrame::SimpleString(Bytes::from_static(b"OK")),
                Err(e) => RespFrame::Error(Bytes::from(e.to_string())),
            }
        }
        Command::LTrim(key, start, stop) => {
            match db.ltrim(&key, start, stop) {
                Ok(_) => RespFrame::SimpleString(Bytes::from_static(b"OK")),
                Err(e) => RespFrame::Error(Bytes::from(e.to_string())),
            }
        }
        Command::LRange(key, start, stop) => {
            match db.lrange(&key, start, stop) {
                Ok(items) => {
                    let array = items.into_iter().map(|item| Box::new(RespFrame::Bulk(item))).collect();
                    RespFrame::Array(array)
                }
                Err(e) => RespFrame::Error(Bytes::from(e.to_string())),
            }
        }
        Command::Unknown(cmd) => {
            let err_msg = format!("ERR unknown command `{}`", String::from_utf8_lossy(&cmd));
            RespFrame::Error(Bytes::from(err_msg))
        }
    }
}