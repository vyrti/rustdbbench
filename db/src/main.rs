use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;
use rustc_hash::{FxHashMap, FxHasher};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

const IN_MEMORY_SHARDS: usize = 128;

//================================================================
// Command Line Interface
//================================================================

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = "A high-performance in-memory TCP database server.")]
struct Cli {
    #[clap(short, long, default_value = "127.0.0.1:7878")]
    addr: String,
}

//================================================================
// In-Memory Key-Value Store (reused from your benchmark)
//================================================================

/// The trait defining the key-value store operations.
#[async_trait]
pub trait KvStore: Send + Sync {
    async fn set(&self, key: String, value: Vec<u8>) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    async fn lpush(&self, key: &str, value: Vec<u8>) -> Result<()>;
    async fn ltrim(&self, key: &str, start: isize, stop: isize) -> Result<()>;
    async fn lrange(&self, key: &str, start: isize, stop: isize) -> Result<Vec<Vec<u8>>>;
}

/// A sharded, in-memory key-value store.
type Shard = FxHashMap<String, Vec<Vec<u8>>>;

#[derive(Clone)]
pub struct InMemoryStore {
    shards: Arc<Vec<Mutex<Shard>>>,
}

impl InMemoryStore {
    /// Creates a new, empty, sharded in-memory store.
    pub fn new() -> Self {
        let mut shards = Vec::with_capacity(IN_MEMORY_SHARDS);
        for _ in 0..IN_MEMORY_SHARDS {
            shards.push(Mutex::new(FxHashMap::default()));
        }
        Self {
            shards: Arc::new(shards),
        }
    }

    /// Calculates the shard index for a given key.
    fn get_shard_index<K: Hash>(&self, key: &K) -> usize {
        let mut hasher = FxHasher::default();
        key.hash(&mut hasher);
        hasher.finish() as usize % IN_MEMORY_SHARDS
    }
}

#[async_trait]
impl KvStore for InMemoryStore {
    /// SET key value
    /// Note: For simplicity, this SET overwrites any existing value, including lists.
    async fn set(&self, key: String, value: Vec<u8>) -> Result<()> {
        let index = self.get_shard_index(&key);
        let mut shard = self.shards[index].lock().unwrap();
        shard.insert(key, vec![value]);
        Ok(())
    }

    /// GET key
    /// Note: If the key holds a list, this gets the first item.
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let index = self.get_shard_index(&key);
        let shard = self.shards[index].lock().unwrap();
        Ok(shard.get(key).and_then(|v| v.first().cloned()))
    }

    /// LPUSH key value
    async fn lpush(&self, key: &str, value: Vec<u8>) -> Result<()> {
        let index = self.get_shard_index(&key);
        let mut shard = self.shards[index].lock().unwrap();
        let list = shard.entry(key.to_string()).or_default();
        list.insert(0, value);
        Ok(())
    }

    /// LTRIM key start stop
    async fn ltrim(&self, key: &str, start: isize, stop: isize) -> Result<()> {
        let index = self.get_shard_index(&key);
        let mut shard = self.shards[index].lock().unwrap();
        if let Some(list) = shard.get_mut(key) {
            if start == 0 && stop >= 0 {
                let end = (stop + 1) as usize;
                list.truncate(end);
            }
        }
        Ok(())
    }

    /// LRANGE key start stop
    async fn lrange(&self, key: &str, start: isize, stop: isize) -> Result<Vec<Vec<u8>>> {
        let index = self.get_shard_index(&key);
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
// Server & Protocol Handling
//================================================================

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let listener = TcpListener::bind(&cli.addr).await?;
    println!("In-Memory DB Server listening on {}", cli.addr);

    let db = InMemoryStore::new();

    loop {
        let (stream, addr) = listener.accept().await?;
        let db_clone = db.clone();

        println!("Accepted connection from: {}", addr);

        tokio::spawn(async move {
            if let Err(e) = handle_connection(db_clone, stream).await {
                println!("Error handling connection from {}: {}", addr, e);
            }
            println!("Closed connection from: {}", addr);
        });
    }
}

/// Handles a single client connection.
async fn handle_connection(db: InMemoryStore, stream: TcpStream) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut buf_reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        let bytes_read = buf_reader.read_line(&mut line).await?;
        if bytes_read == 0 {
            // Connection closed by client
            break;
        }

        let trimmed_line = line.trim();
        let mut parts = trimmed_line.splitn(3, ' ');
        let command = parts.next().unwrap_or("").to_uppercase();

        match command.as_str() {
            "GET" => {
                if let Some(key) = parts.next() {
                    match db.get(key).await? {
                        Some(value) => {
                            // Respond with RESP Bulk String
                            writer.write_all(format!("${}\r\n", value.len()).as_bytes()).await?;
                            writer.write_all(&value).await?;
                            writer.write_all(b"\r\n").await?;
                        }
                        None => {
                            // Respond with RESP Null Bulk String
                            writer.write_all(b"$-1\r\n").await?;
                        }
                    }
                } else {
                    writer.write_all(b"-ERR wrong number of arguments for 'get' command\r\n").await?;
                }
            }
            "SET" => {
                let key = parts.next();
                let value = parts.next();
                if let (Some(k), Some(v)) = (key, value) {
                    db.set(k.to_string(), v.as_bytes().to_vec()).await?;
                    writer.write_all(b"+OK\r\n").await?;
                } else {
                     writer.write_all(b"-ERR wrong number of arguments for 'set' command\r\n").await?;
                }
            }
            "LPUSH" => {
                let key = parts.next();
                let value = parts.next();
                if let (Some(k), Some(v)) = (key, value) {
                    db.lpush(k, v.as_bytes().to_vec()).await?;
                     writer.write_all(b"+OK\r\n").await?;
                } else {
                     writer.write_all(b"-ERR wrong number of arguments for 'lpush' command\r\n").await?;
                }
            }
            "LRANGE" => {
                 let key = parts.next();
                 let start_str = parts.next();
                 let stop_str = parts.next();
                 if let (Some(k), Some(s_str), Some(e_str)) = (key, start_str, stop_str) {
                     if let (Ok(start), Ok(stop)) = (s_str.parse::<isize>(), e_str.parse::<isize>()) {
                         let values = db.lrange(k, start, stop).await?;
                         // Respond with RESP Array
                         writer.write_all(format!("*{}\r\n", values.len()).as_bytes()).await?;
                         for val in values {
                             writer.write_all(format!("${}\r\n", val.len()).as_bytes()).await?;
                             writer.write_all(&val).await?;
                             writer.write_all(b"\r\n").await?;
                         }
                     } else {
                         writer.write_all(b"-ERR value is not an integer or out of range\r\n").await?;
                     }
                 } else {
                      writer.write_all(b"-ERR wrong number of arguments for 'lrange' command\r\n").await?;
                 }
            }
            "" => { /* Ignore empty lines */ }
            _ => {
                writer.write_all(format!("-ERR unknown command `{}`\r\n", command).as_bytes()).await?;
            }
        }
    }
    Ok(())
}