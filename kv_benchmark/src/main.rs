use anyhow::Result;
use async_trait::async_trait;
use byte_unit::{Byte, UnitType};
use clap::{Parser, ValueEnum};
use futures::stream::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use rand::{distributions::Alphanumeric, prelude::*, rngs::StdRng, SeedableRng};
use redis::{aio::MultiplexedConnection, AsyncCommands};
use rkyv::{
    access, // Used for validation/deserialization with `Archived`
    rancor::Error as RkyvError, // Renamed to avoid conflict with anyhow::Error
    to_bytes, // For serializing to bytes
    Archive, // For deriving Archive trait
    Deserialize as RkyvDeserialize, // For deserializing archived types
    Serialize as RkyvSerialize, // For serializing to archived types
    Archived, // Needed for `access::<Archived<T>, E>`
};
use rustc_hash::{FxHashMap, FxHasher};
use serde::{Deserialize as SerdeDeserialize, Serialize};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use zstd::stream::{decode_all, encode_all};

use bitcode;
use prost::Message;

// --- Imports for Internal Pub/Sub ---
use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::{broadcast, mpsc};

pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/benchmark.rs"));
}
#[allow(dead_code, unused_imports)]
pub mod fbs {
    include!(concat!(env!("OUT_DIR"), "/benchmark_generated.rs"));
}

const DEFAULT_KEY_PREFIX: &str = "bench";
const DEFAULT_BATCH_SIZE: usize = 50;
const PROGRESS_UPDATE_INTERVAL: usize = 1000;
const IN_MEMORY_SHARDS: usize = 128;

#[derive(ValueEnum, Clone, Debug, Copy, PartialEq, Eq)]
enum DataFormat {
    String, Json, Bitcode, Protobuf, Rkyv, Flatbuffers,
}

#[derive(ValueEnum, Clone, Debug, Copy, PartialEq, Eq)]
enum Workload {
    Chat, Simple,
}

#[derive(ValueEnum, Clone, Debug, Copy, PartialEq, Eq)]
enum DbChoice {
    Redis, Valkey, InMemory, RustDb,
}

#[derive(Parser, Debug, Clone)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(long, value_enum, default_value_t = DbChoice::Redis, help = "Database backend to use.")]
    db: DbChoice,
    #[clap(short, long, default_value = "redis://127.0.0.1:6379")]
    redis_url: String,
    #[clap(short, long, default_value = "redis://127.0.0.1:6380")]
    valkey_url: String,
    #[clap(long, default_value = "redis://127.0.0.1:7878", help="URL for the custom Rust Redis server.")]
    rustdb_url: String,
    #[clap(long, value_enum, default_value_t = Workload::Chat, help = "The type of benchmark workload to run.")]
    workload: Workload,
    #[clap(short = 'o', long, default_value_t = 100_000, help="Total number of operations to perform.")]
    num_ops: usize,
    #[clap(short, long, default_value_t = 50, help="Number of concurrent client threads.")]
    concurrency: usize,
    #[clap(long, default_value_t = 128, help="Base size in bytes for the data part of a message payload.")]
    value_size: usize,
    #[clap(long, value_enum, default_value_t = DataFormat::String, help = "Data serialization format for values")]
    format: DataFormat,
    #[clap(long, default_value_t = 1000, help="For Chat workload: number of simulated chat rooms.")]
    num_chats: usize,
    #[clap(long, default_value_t = 100, help="For Chat workload: number of messages to keep in chat history (for LTRIM).")]
    history_len: usize,
    #[clap(long, default_value_t = 50, help="For Chat workload: number of messages to fetch in a single READ operation (for LRANGE).")]
    read_size: usize,
    #[clap(long, help = "Run only write benchmarks")]
    write_only: bool,
    #[clap(long, help = "Run only read benchmarks")]
    read_only: bool,
    #[clap(long, help = "Run only Pub/Sub benchmarks")]
    pubsub_only: bool,
    #[clap(long, help = "For Pub/Sub: number of publisher clients", default_value_t = 1)]
    num_publishers: usize,
    #[clap(long, help = "Use explicit pipelining for commands")]
    pipeline: bool,
    #[clap(long, default_value_t = DEFAULT_BATCH_SIZE, help = "Batch size for pipelined commands")]
    batch_size: usize,
    #[clap(long, help = "Skip latency tracking (useful for max throughput tests or pipelined mode)")]
    no_latency: bool,
    #[clap(long, help = "Enable zstd compression for values")]
    compress_zstd: bool,
}

#[derive(Serialize, SerdeDeserialize, Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone)]
struct BenchmarkPayload {
    data: String,
    timestamp: u64,
    id: usize,
}

// --- Abstraction over the database ---
#[async_trait]
trait KvStore: Send + Sync {
    fn clone_box(&self) -> Box<dyn KvStore + Send + Sync>;
    async fn set(&self, key: String, value: Vec<u8>) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    async fn lpush(&self, key: &str, value: Vec<u8>) -> Result<()>;
    async fn ltrim(&self, key: &str, start: isize, stop: isize) -> Result<()>;
    async fn lrange(&self, key: &str, start: isize, stop: isize) -> Result<Vec<Vec<u8>>>;
    async fn batch_set(&self, items: Vec<(String, Vec<u8>)>) -> Result<()>;
    async fn batch_get(&self, keys: Vec<&str>) -> Result<Vec<Vec<u8>>>;
    async fn batch_lpush_ltrim(&self, items: Vec<(&str, Vec<u8>)>, history_len: isize) -> Result<()>;
    async fn batch_lrange(&self, keys: Vec<&str>, start: isize, stop: isize) -> Result<Vec<Vec<Vec<u8>>>>;
}

impl Clone for Box<dyn KvStore + Send + Sync> {
    fn clone(&self) -> Box<dyn KvStore + Send + Sync> {
        self.clone_box()
    }
}

// --- High-Performance In-Memory Database Implementation ---
type Shard = FxHashMap<String, Vec<Vec<u8>>>;

#[derive(Clone)]
struct InMemoryStore {
    shards: Arc<Vec<Mutex<Shard>>>,
}

impl InMemoryStore {
    fn new() -> Self {
        let mut shards = Vec::with_capacity(IN_MEMORY_SHARDS);
        for _ in 0..IN_MEMORY_SHARDS {
            shards.push(Mutex::new(FxHashMap::default()));
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
    fn clone_box(&self) -> Box<dyn KvStore + Send + Sync> { Box::new(self.clone()) }
    async fn set(&self, key: String, value: Vec<u8>) -> Result<()> {
        let index = self.get_shard_index(&key);
        let mut shard = self.shards[index].lock().unwrap();
        shard.insert(key, vec![value]);
        Ok(())
    }
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let index = self.get_shard_index(&key);
        let shard = self.shards[index].lock().unwrap();
        Ok(shard.get(key).and_then(|v| v.first().cloned()))
    }
    async fn lpush(&self, key: &str, value: Vec<u8>) -> Result<()> {
        let index = self.get_shard_index(&key);
        let mut shard = self.shards[index].lock().unwrap();
        let list = shard.entry(key.to_string()).or_default();
        list.insert(0, value);
        Ok(())
    }
    async fn ltrim(&self, key: &str, start: isize, stop: isize) -> Result<()> {
        let index = self.get_shard_index(&key);
        let mut shard = self.shards[index].lock().unwrap();
        if let Some(list) = shard.get_mut(key) {
            if start == 0 && stop >= 0 {
                list.truncate((stop + 1) as usize);
            }
        }
        Ok(())
    }
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
    async fn batch_set(&self, items: Vec<(String, Vec<u8>)>) -> Result<()> {
        for (key, value) in items { self.set(key, value).await?; }
        Ok(())
    }
    async fn batch_get(&self, keys: Vec<&str>) -> Result<Vec<Vec<u8>>> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.get(key).await?.unwrap_or_default());
        }
        Ok(results)
    }
    async fn batch_lpush_ltrim(&self, items: Vec<(&str, Vec<u8>)>, history_len: isize) -> Result<()> {
        for (key, value) in items {
            self.lpush(key, value).await?;
            self.ltrim(key, 0, history_len - 1).await?;
        }
        Ok(())
    }
    async fn batch_lrange(&self, keys: Vec<&str>, start: isize, stop: isize) -> Result<Vec<Vec<Vec<u8>>>> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.lrange(key, start, stop).await?);
        }
        Ok(results)
    }
}

// --- Redis Implementation of the Trait ---
// NOTE: This single implementation now works for Redis, Valkey, and our RustDb server!
#[derive(Clone)]
struct RedisStore {
    conn: MultiplexedConnection,
}

impl RedisStore {
    async fn new(url: &str) -> Result<Self> {
        let client = redis::Client::open(url)?;
        Ok(Self { conn: client.get_multiplexed_async_connection().await? })
    }
}

#[async_trait]
impl KvStore for RedisStore {
    fn clone_box(&self) -> Box<dyn KvStore + Send + Sync> { Box::new(self.clone()) }
    async fn set(&self, key: String, value: Vec<u8>) -> Result<()> {
        let mut conn = self.conn.clone();
        conn.set::<_, _, ()>(key, value).await?;
        Ok(())
    }
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let mut conn = self.conn.clone();
        Ok(conn.get(key).await?)
    }
    async fn lpush(&self, key: &str, value: Vec<u8>) -> Result<()> {
        let mut conn = self.conn.clone();
        conn.lpush::<_, _, ()>(key, value).await?;
        Ok(())
    }
    async fn ltrim(&self, key: &str, start: isize, stop: isize) -> Result<()> {
        let mut conn = self.conn.clone();
        conn.ltrim::<_, ()>(key, start, stop).await?;
        Ok(())
    }
    async fn lrange(&self, key: &str, start: isize, stop: isize) -> Result<Vec<Vec<u8>>> {
        let mut conn = self.conn.clone();
        Ok(conn.lrange(key, start, stop).await?)
    }
    async fn batch_set(&self, items: Vec<(String, Vec<u8>)>) -> Result<()> {
        let mut conn = self.conn.clone();
        let mut pipe = redis::pipe();
        for (key, value) in items {
            pipe.set(key, value).ignore();
        }
        pipe.query_async::<()>(&mut conn).await?;
        Ok(())
    }
    async fn batch_get(&self, keys: Vec<&str>) -> Result<Vec<Vec<u8>>> {
        let mut conn = self.conn.clone();
        let mut pipe = redis::pipe();
        for key in keys {
            pipe.get(key);
        }
        Ok(pipe.query_async(&mut conn).await?)
    }
    async fn batch_lpush_ltrim(&self, items: Vec<(&str, Vec<u8>)>, history_len: isize) -> Result<()> {
        let mut conn = self.conn.clone();
        let mut pipe = redis::pipe();
        for (key, value) in items {
            pipe.lpush(key, value).ltrim(key, 0, history_len - 1).ignore();
        }
        pipe.query_async::<()>(&mut conn).await?;
        Ok(())
    }
    async fn batch_lrange(&self, keys: Vec<&str>, start: isize, stop: isize) -> Result<Vec<Vec<Vec<u8>>>> {
        let mut conn = self.conn.clone();
        let mut pipe = redis::pipe();
        for key in keys {
            pipe.lrange(key, start, stop);
        }
        Ok(pipe.query_async(&mut conn).await?)
    }
}

// --- Data Generation & Other Structs ---
fn generate_random_string(len: usize, rng: &mut impl Rng) -> String {
    let mut s = String::with_capacity(len);
    (0..len).for_each(|_| s.push(Alphanumeric.sample(rng) as char));
    s
}

struct PreGeneratedData {
    keys: Arc<Vec<String>>,
    values: Option<Arc<Vec<Vec<u8>>>>,
}

impl PreGeneratedData {
    fn new(cli: &Cli, op_type_for_values: &str) -> Self {
        let mut rng = StdRng::from_entropy();
        let num_keys = if cli.workload == Workload::Chat { cli.num_chats } else { cli.num_ops };
        println!("Pre-generating {} keys and potentially {} message values (base size {}B)...", num_keys, if op_type_for_values != "READ" { cli.num_ops } else { 0 }, cli.value_size);
        let key_prefix = match cli.workload { Workload::Chat => "chat", Workload::Simple => "simple", };
        let keys = Arc::new((0..num_keys).map(|i| format!("{}:{}:{}", key_prefix, i, generate_random_string(10, &mut rng))).collect::<Vec<_>>());
        let values = if op_type_for_values != "READ" {
            let values_bytes: Vec<Vec<u8>> = (0..cli.num_ops).map(|i| {
                let random_data = generate_random_string(cli.value_size, &mut rng);
                let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                let serialized_bytes = match cli.format {
                    DataFormat::String => random_data.into_bytes(),
                    DataFormat::Json => serde_json::to_vec(&BenchmarkPayload { data: random_data, timestamp, id: i }).unwrap(),
                    DataFormat::Bitcode => bitcode::serialize(&BenchmarkPayload { data: random_data, timestamp, id: i }).unwrap(),
                    DataFormat::Protobuf => pb::BenchmarkPayload { data: random_data, timestamp, id: i as u64 }.encode_to_vec(),
                    DataFormat::Rkyv => to_bytes::<RkyvError>(&BenchmarkPayload { data: random_data, timestamp, id: i }).unwrap().into_vec(),
                    DataFormat::Flatbuffers => {
                        let mut builder = flatbuffers::FlatBufferBuilder::new();
                        let data_offset = builder.create_string(&random_data);
                        let mut payload_builder = fbs::benchmark_fbs::BenchmarkPayloadBuilder::new(&mut builder);
                        payload_builder.add_data(data_offset);
                        payload_builder.add_timestamp(timestamp);
                        payload_builder.add_id(i as u64);
                        let payload_offset = payload_builder.finish();
                        builder.finish(payload_offset, None);
                        builder.finished_data().to_vec()
                    }
                };
                if cli.compress_zstd { encode_all(serialized_bytes.as_slice(), 0).unwrap() } else { serialized_bytes }
            }).collect();
            if !values_bytes.is_empty() {
                let total_bytes: usize = values_bytes.iter().map(|v| v.len()).sum();
                println!("Pre-generated {} values. Average serialized value size{}: {:.2} B.", values_bytes.len(), if cli.compress_zstd { " (compressed)" } else { "" }, total_bytes as f64 / values_bytes.len() as f64);
            }
            Some(Arc::new(values_bytes))
        } else { None };
        println!("Data generation complete.");
        Self { keys, values }
    }
}

struct BenchResult {
    ops_per_second: f64,
    total_time: Duration,
    avg_latency_ms: Option<f64>,
    p99_latency_ms: Option<f64>,
    errors: usize,
    total_bytes_written: u64,
    total_bytes_read: u64,
}

struct WorkerResult {
    histogram: Option<hdrhistogram::Histogram<u64>>,
    errors: usize,
    ops_done: usize,
    bytes_written: u64,
    bytes_read: u64,
}

async fn run_pubsub_benchmark(db_name: &str, db_url_slice: &str, cli: &Cli, data: &PreGeneratedData, track_latency: bool) -> Result<BenchResult> {
    let num_publishers = cli.num_publishers;
    let num_subscribers = cli.concurrency.saturating_sub(num_publishers);
    if num_subscribers == 0 { return Err(anyhow::anyhow!("Pub/Sub benchmark requires at least one subscriber.")); }
    if num_publishers == 0 { return Err(anyhow::anyhow!("Pub/Sub benchmark requires at least one publisher.")); }
    println!("\nBenchmarking {} PUBSUB ({} pubs, {} subs, {} ops total)...", db_name, num_publishers, num_subscribers, cli.num_ops);
    let client = redis::Client::open(db_url_slice)?;
    let pb = ProgressBar::new(cli.num_ops as u64);
    pb.set_style(ProgressStyle::default_bar().template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}").unwrap().progress_chars("#>-"));
    pb.set_message(format!("{} PUBSUB", db_name));
    let pb_arc = Arc::new(pb);
    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
    let ready_barrier = Arc::new(tokio::sync::Barrier::new(num_subscribers + 1));
    let received_msg_count = Arc::new(AtomicUsize::new(0));
    let received_bytes_count = Arc::new(AtomicU64::new(0));
    let mut sub_tasks = Vec::with_capacity(num_subscribers);
    for _ in 0..num_subscribers {
        let sub_client = client.clone();
        let barrier = ready_barrier.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        let msg_count = received_msg_count.clone();
        let bytes_count = received_bytes_count.clone();
        let channel_pattern = format!("{}:*", DEFAULT_KEY_PREFIX);
        sub_tasks.push(tokio::spawn(async move {
            let mut pubsub = sub_client.get_async_pubsub().await.unwrap();
            pubsub.psubscribe(&channel_pattern).await.unwrap();
            barrier.wait().await;
            let mut message_stream = pubsub.on_message();
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown_rx.recv() => break,
                    Some(msg) = message_stream.next() => {
                        msg_count.fetch_add(1, Ordering::Relaxed);
                        bytes_count.fetch_add(msg.get_payload_bytes().len() as u64, Ordering::Relaxed);
                    },
                    else => break,
                }
            }
        }));
    }
    ready_barrier.wait().await;
    let start_time = Instant::now();
    let mut pub_tasks = Vec::with_capacity(num_publishers);
    let ops_per_publisher = (cli.num_ops + num_publishers - 1) / num_publishers;
    for worker_id in 0..num_publishers {
        let mut conn = client.get_multiplexed_async_connection().await?;
        let keys_ref = Arc::clone(&data.keys);
        let values_ref = data.values.as_ref().unwrap().clone();
        let progress_bar_clone = pb_arc.clone();
        let worker_start_idx = worker_id * ops_per_publisher;
        let worker_end_idx = ((worker_id + 1) * ops_per_publisher).min(cli.num_ops);
        if worker_start_idx >= worker_end_idx { continue; }
        pub_tasks.push(tokio::spawn(async move {
            let mut hist = if track_latency { Some(hdrhistogram::Histogram::<u64>::new(3).unwrap()) } else { None };
            let mut local_errors = 0;
            let mut local_ops_done = 0;
            let mut local_bytes_written = 0;
            let mut rng = StdRng::from_entropy();
            for i in worker_start_idx..worker_end_idx {
                let key = &keys_ref[rng.gen_range(0..keys_ref.len())];
                let value = &values_ref[i];
                let op_start_time = if track_latency { Some(Instant::now()) } else { None };
                if conn.publish::<_, _, ()>(key, value).await.is_ok() {
                    local_ops_done += 1;
                    local_bytes_written += value.len() as u64;
                } else { local_errors += 1; }
                if let (Some(st), Some(h)) = (op_start_time, hist.as_mut()) { h.record(st.elapsed().as_micros() as u64).unwrap(); }
            }
            progress_bar_clone.inc((worker_end_idx - worker_start_idx) as u64);
            WorkerResult { histogram: hist, errors: local_errors, ops_done: local_ops_done, bytes_written: local_bytes_written, bytes_read: 0 }
        }));
    }
    let publisher_results = futures::future::join_all(pub_tasks).await;
    let total_time = start_time.elapsed();
    let _ = shutdown_tx.send(());
    pb_arc.finish_with_message("Done");
    let _ = futures::future::join_all(sub_tasks).await;
    let mut final_histogram = if track_latency { Some(hdrhistogram::Histogram::<u64>::new(3).unwrap()) } else { None };
    let mut total_errors = 0;
    let mut total_bytes_written = 0;
    for result in publisher_results {
        if let Ok(wr) = result {
            if let (Some(fh), Some(wh)) = (final_histogram.as_mut(), wr.histogram.as_ref()) { fh.add(wh).unwrap(); }
            total_errors += wr.errors;
            total_bytes_written += wr.bytes_written;
        }
    }
    let total_bytes_read = received_bytes_count.load(Ordering::SeqCst);
    let successful_ops = cli.num_ops.saturating_sub(total_errors);
    let ops_per_second = if total_time.as_secs_f64() > 0.0 { successful_ops as f64 / total_time.as_secs_f64() } else { 0.0 };
    let (avg_lat, p99_lat) = if let Some(hist) = final_histogram.as_ref() { (Some(hist.mean() / 1000.0), Some(hist.value_at_percentile(99.0) as f64 / 1000.0)) } else { (None, None) };
    Ok(BenchResult { ops_per_second, total_time, avg_latency_ms: avg_lat, p99_latency_ms: p99_lat, errors: total_errors, total_bytes_written, total_bytes_read })
}

async fn run_internal_pubsub_benchmark(
    db_name: &str,
    cli: &Cli,
    data: &PreGeneratedData,
    track_latency: bool,
) -> Result<BenchResult> {
    let num_publishers = cli.num_publishers;
    let num_subscribers = cli.concurrency.saturating_sub(num_publishers);
    if num_subscribers == 0 {
        return Err(anyhow::anyhow!(
            "Pub/Sub benchmark requires at least one subscriber."
        ));
    }
    if num_publishers == 0 {
        return Err(anyhow::anyhow!(
            "Pub/Sub benchmark requires at least one publisher."
        ));
    }
    println!("\nBenchmarking {} ZERO-COPY INTERNAL PUBSUB ({} pubs, {} subs, {} ops total)...", db_name, num_publishers, num_subscribers, cli.num_ops);

    type SharedBytes = Arc<Bytes>;
    type MessageSender = mpsc::Sender<SharedBytes>;
    type Subscriptions = Arc<DashMap<String, Vec<MessageSender>>>;

    let subscriptions: Subscriptions = Arc::new(DashMap::new());
    let pb = ProgressBar::new(cli.num_ops as u64);
    pb.set_style(ProgressStyle::default_bar().template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}").unwrap().progress_chars("#>-"));
    pb.set_message(format!("{} Zero-Copy PUBSUB", db_name));
    let pb_arc = Arc::new(pb);

    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    let received_msg_count = Arc::new(AtomicUsize::new(0));
    let received_bytes_count = Arc::new(AtomicU64::new(0));
    let ready_barrier = Arc::new(tokio::sync::Barrier::new(
        num_subscribers + num_publishers,
    ));

    let mut sub_tasks = Vec::with_capacity(num_subscribers);
    for _ in 0..num_subscribers {
        let subs_clone = subscriptions.clone();
        let keys_ref = Arc::clone(&data.keys);
        let msg_count = received_msg_count.clone();
        let bytes_count = received_bytes_count.clone();
        let barrier = ready_barrier.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        let cli_format = cli.format;

        sub_tasks.push(tokio::spawn(async move {
            let (tx, mut rx) = mpsc::channel::<SharedBytes>(256);
            for key in keys_ref.iter() {
                // This will add a sender clone for every key to every subscriber
                subs_clone.entry(key.clone()).or_default().push(tx.clone());
            }

            barrier.wait().await;
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown_rx.recv() => break,
                    Some(msg_arc) = rx.recv() => {
                        msg_count.fetch_add(1, Ordering::Relaxed);
                        bytes_count.fetch_add(msg_arc.len() as u64, Ordering::Relaxed);
                        // Process message based on format to simulate read/deserialization cost.
                        let _ = process_read_result_simple((*msg_arc).to_vec(), cli_format, false);
                    },
                    else => break,
                }
            }
        }));
    }

    let mut pub_tasks = Vec::with_capacity(num_publishers);
    let ops_per_publisher = (cli.num_ops + num_publishers - 1) / num_publishers;

    for worker_id in 0..num_publishers {
        let subs_clone = subscriptions.clone();
        let keys_ref = Arc::clone(&data.keys);
        let cli_clone = cli.clone();
        let progress_bar_clone = pb_arc.clone();
        let barrier = ready_barrier.clone();
        let worker_start_idx = worker_id * ops_per_publisher;
        let worker_end_idx = ((worker_id + 1) * ops_per_publisher).min(cli.num_ops);
        if worker_start_idx >= worker_end_idx {
            continue;
        }

        pub_tasks.push(tokio::spawn(async move {
            let mut hist = if track_latency {
                Some(hdrhistogram::Histogram::<u64>::new(3).unwrap())
            } else {
                None
            };
            let mut local_ops_done = 0;
            let mut local_bytes_written = 0;
            let mut rng = StdRng::from_entropy();

            barrier.wait().await;
            for i in worker_start_idx..worker_end_idx {
                let key = &keys_ref[rng.gen_range(0..keys_ref.len())];

                let random_data = generate_random_string(cli_clone.value_size, &mut rng);
                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();

                // Serialize the payload according to the selected format
                let serialized_bytes = match cli_clone.format {
                    DataFormat::String => random_data.into_bytes(),
                    DataFormat::Json => serde_json::to_vec(&BenchmarkPayload {
                        data: random_data,
                        timestamp,
                        id: i,
                    })
                    .unwrap(),
                    DataFormat::Bitcode => bitcode::serialize(&BenchmarkPayload {
                        data: random_data,
                        timestamp,
                        id: i,
                    })
                    .unwrap(),
                    DataFormat::Protobuf => pb::BenchmarkPayload {
                        data: random_data,
                        timestamp,
                        id: i as u64,
                    }
                    .encode_to_vec(),
                    DataFormat::Rkyv => to_bytes::<RkyvError>(&BenchmarkPayload { data: random_data, timestamp, id: i }).unwrap().into_vec(),
                    DataFormat::Flatbuffers => {
                        let mut builder = flatbuffers::FlatBufferBuilder::new();
                        let data_offset = builder.create_string(&random_data);
                        let mut payload_builder =
                            fbs::benchmark_fbs::BenchmarkPayloadBuilder::new(&mut builder);
                        payload_builder.add_data(data_offset);
                        payload_builder.add_timestamp(timestamp);
                        payload_builder.add_id(i as u64);
                        let payload_offset = payload_builder.finish();
                        builder.finish(payload_offset, None);
                        builder.finished_data().to_vec()
                    }
                };

                let value_bytes: SharedBytes = Arc::new(Bytes::from(serialized_bytes));

                let op_start_time = if track_latency { Some(Instant::now()) } else { None };

                if let Some(subscribers) = subs_clone.get(key) {
                    local_bytes_written += (value_bytes.len() * subscribers.len()) as u64;
                    for sender in subscribers.iter() {
                        if sender.send(value_bytes.clone()).await.is_err() {}
                    }
                }

                if let (Some(st), Some(h)) = (op_start_time, hist.as_mut()) {
                    h.record(st.elapsed().as_micros() as u64).unwrap();
                }
                local_ops_done += 1;
            }
            progress_bar_clone.inc((worker_end_idx - worker_start_idx) as u64);
            WorkerResult {
                histogram: hist,
                errors: 0,
                ops_done: local_ops_done,
                bytes_written: local_bytes_written,
                bytes_read: 0,
            }
        }));
    }

    let start_time = Instant::now();
    let publisher_results = futures::future::join_all(pub_tasks).await;
    let total_time = start_time.elapsed();

    let _ = shutdown_tx.send(());
    pb_arc.finish_with_message("Done");
    let _ = futures::future::join_all(sub_tasks).await;

    let mut final_histogram = if track_latency {
        Some(hdrhistogram::Histogram::<u64>::new(3).unwrap())
    } else {
        None
    };
    let mut total_errors = 0;
    let mut total_bytes_written = 0;
    for result in publisher_results {
        if let Ok(wr) = result {
            if let (Some(fh), Some(wh)) = (final_histogram.as_mut(), wr.histogram.as_ref()) {
                fh.add(wh).unwrap();
            }
            total_errors += wr.errors;
            total_bytes_written += wr.bytes_written;
        }
    }
    let total_bytes_read = received_bytes_count.load(Ordering::SeqCst);
    let successful_ops = cli.num_ops.saturating_sub(total_errors);
    let ops_per_second = if total_time.as_secs_f64() > 0.0 {
        successful_ops as f64 / total_time.as_secs_f64()
    } else {
        0.0
    };
    let (avg_lat, p99_lat) = if let Some(hist) = final_histogram.as_ref() {
        (
            Some(hist.mean() / 1000.0),
            Some(hist.value_at_percentile(99.0) as f64 / 1000.0),
        )
    } else {
        (None, None)
    };
    Ok(BenchResult {
        ops_per_second,
        total_time,
        avg_latency_ms: avg_lat,
        p99_latency_ms: p99_lat,
        errors: total_errors,
        total_bytes_written,
        total_bytes_read,
    })
}


async fn run_benchmark(db_name: &str, op_type_ref: &str, db: Box<dyn KvStore + Send + Sync>, cli: &Cli, data: &PreGeneratedData, track_latency: bool) -> Result<BenchResult> {
    let op_type_owned = op_type_ref.to_string();
    let _mode_str = if cli.pipeline { "PIPELINED" } else { "INDIVIDUAL" };
    let zero_copy_description = if op_type_owned == "READ" && matches!(cli.format, DataFormat::Rkyv | DataFormat::Flatbuffers) { " (Zero-Copy)" } else { "" };
    println!("\nBenchmarking {} {} ({:?} workload){}{}...", db_name, op_type_owned, cli.workload, zero_copy_description, if op_type_owned == "READ" && cli.workload == Workload::Chat { format!(" READ_SIZE: {}", cli.read_size) } else { "".to_string() });
    let pb = ProgressBar::new(cli.num_ops as u64);
    pb.set_style(ProgressStyle::default_bar().template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}").unwrap().progress_chars("#>-"));
    pb.set_message(format!("{} {} ({:?}){}", db_name, op_type_owned, cli.workload, zero_copy_description));
    let pb_arc = Arc::new(pb);
    let start_time = Instant::now();
    let mut tasks = Vec::with_capacity(cli.concurrency);
    let ops_per_worker = (cli.num_ops + cli.concurrency - 1) / cli.concurrency;
    for worker_id in 0..cli.concurrency {
        let db_clone = db.clone();
        let keys_ref = Arc::clone(&data.keys);
        let values_ref = data.values.as_ref().map(Arc::clone);
        let progress_bar_clone = Arc::clone(&pb_arc);
        let cli_clone = cli.clone();
        let task_op_type = op_type_owned.clone();
        let worker_start_idx = worker_id * ops_per_worker;
        let worker_end_idx = ((worker_id + 1) * ops_per_worker).min(cli.num_ops);
        if worker_start_idx >= worker_end_idx { continue; }
        tasks.push(tokio::spawn(async move {
            let mut hist = if track_latency && !cli_clone.pipeline { Some(hdrhistogram::Histogram::<u64>::new(3).unwrap()) } else { None };
            let mut local_errors = 0;
            let mut local_ops_done = 0;
            let mut local_bytes_written = 0;
            let mut local_bytes_read = 0;
            let num_worker_ops = worker_end_idx - worker_start_idx;
            let mut rng = StdRng::from_entropy();
            if cli_clone.pipeline {
                for chunk_start_offset in (0..num_worker_ops).step_by(cli_clone.batch_size) {
                    let current_batch_size = (chunk_start_offset + cli_clone.batch_size).min(num_worker_ops) - chunk_start_offset;
                    if current_batch_size == 0 { continue; }
                    let result = match cli_clone.workload {
                        Workload::Chat => if task_op_type == "WRITE" {
                            let items = (0..current_batch_size).map(|i| { let op_idx = worker_start_idx + chunk_start_offset + i; let key = &keys_ref[rng.gen_range(0..keys_ref.len())]; let value = values_ref.as_ref().unwrap()[op_idx].clone(); local_bytes_written += value.len() as u64; (key.as_str(), value) }).collect();
                            db_clone.batch_lpush_ltrim(items, cli_clone.history_len as isize).await
                        } else {
                            let keys = (0..current_batch_size).map(|_| keys_ref[rng.gen_range(0..keys_ref.len())].as_str()).collect();
                            match db_clone.batch_lrange(keys, 0, cli_clone.read_size as isize - 1).await {
                                Ok(results) => { local_bytes_read += results.iter().flat_map(|m| m.iter().map(|b| b.len() as u64)).sum::<u64>(); for messages in results { if !process_read_result_chat(messages, cli_clone.format, cli_clone.compress_zstd) { local_errors += 1; } } Ok(()) },
                                Err(e) => { local_errors += current_batch_size; Err(e) }
                            }
                        },
                        Workload::Simple => if task_op_type == "WRITE" {
                            let items = (0..current_batch_size).map(|i| { let op_idx = worker_start_idx + chunk_start_offset + i; let key = keys_ref[op_idx].clone(); let value = values_ref.as_ref().unwrap()[op_idx].clone(); local_bytes_written += value.len() as u64; (key, value) }).collect();
                            db_clone.batch_set(items).await
                        } else {
                            let keys = (0..current_batch_size).map(|i| { let op_idx = worker_start_idx + chunk_start_offset + i; keys_ref[op_idx].as_str() }).collect();
                            match db_clone.batch_get(keys).await {
                                Ok(results) => { local_bytes_read += results.iter().map(|b| b.len() as u64).sum::<u64>(); for bytes in results { if !process_read_result_simple(bytes, cli_clone.format, cli_clone.compress_zstd) { local_errors += 1; } } Ok(()) },
                                Err(e) => { local_errors += current_batch_size; Err(e) }
                            }
                        }
                    };
                    if result.is_err() { local_errors += current_batch_size; }
                    local_ops_done += current_batch_size;
                    progress_bar_clone.inc(current_batch_size as u64);
                }
            } else {
                for i in 0..num_worker_ops {
                    let op_idx = worker_start_idx + i;
                    let op_start_time = if track_latency { Some(Instant::now()) } else { None };
                    let op_successful = match cli_clone.workload {
                        Workload::Chat => {
                            let key = &keys_ref[rng.gen_range(0..keys_ref.len())];
                            if task_op_type == "WRITE" { let value = values_ref.as_ref().unwrap()[op_idx].clone(); local_bytes_written += value.len() as u64; db_clone.lpush(key, value).await.is_ok() && db_clone.ltrim(key, 0, cli_clone.history_len as isize - 1).await.is_ok() }
                            else { match db_clone.lrange(key, 0, cli_clone.read_size as isize - 1).await { Ok(messages) => { local_bytes_read += messages.iter().map(|m| m.len() as u64).sum::<u64>(); process_read_result_chat(messages, cli_clone.format, cli_clone.compress_zstd) }, Err(_) => false } }
                        },
                        Workload::Simple => {
                            let key = &keys_ref[op_idx];
                            if task_op_type == "WRITE" { let value = values_ref.as_ref().unwrap()[op_idx].clone(); local_bytes_written += value.len() as u64; db_clone.set(key.clone(), value).await.is_ok() }
                            else { match db_clone.get(key).await { Ok(Some(bytes)) => { local_bytes_read += bytes.len() as u64; process_read_result_simple(bytes, cli_clone.format, cli_clone.compress_zstd) }, Ok(None) => true, Err(_) => false, } }
                        }
                    };
                    if let Some(st) = op_start_time { if let Some(h) = hist.as_mut() { h.record(st.elapsed().as_micros() as u64).unwrap(); } }
                    if !op_successful { local_errors += 1; }
                    local_ops_done += 1;
                    progress_bar_clone.inc(1);
                }
            }
            WorkerResult { histogram: hist, errors: local_errors, ops_done: local_ops_done, bytes_written: local_bytes_written, bytes_read: local_bytes_read }
        }));
    }
    let worker_results = futures::future::join_all(tasks).await;
    let total_time = start_time.elapsed();
    pb_arc.finish_with_message("Done");
    let mut final_histogram = if track_latency && !cli.pipeline { Some(hdrhistogram::Histogram::<u64>::new(3).unwrap()) } else { None };
    let mut total_errors = 0;
    let mut total_bytes_written = 0;
    let mut total_bytes_read = 0;
    for result in worker_results {
        if let Ok(wr) = result {
            if let (Some(fh), Some(wh)) = (final_histogram.as_mut(), wr.histogram.as_ref()) { fh.add(wh).unwrap(); }
            total_errors += wr.errors;
            total_bytes_written += wr.bytes_written;
            total_bytes_read += wr.bytes_read;
        } else { eprintln!("Worker task panicked."); total_errors += cli.num_ops / cli.concurrency; }
    }
    if total_errors > 0 { println!("WARNING: {}/{} operations reported errors.", total_errors, cli.num_ops); }
    let successful_ops = cli.num_ops.saturating_sub(total_errors);
    let ops_per_second = if total_time.as_secs_f64() > 0.0 { successful_ops as f64 / total_time.as_secs_f64() } else { 0.0 };
    let (avg_lat, p99_lat) = if let Some(hist) = final_histogram.as_ref() { (Some(hist.mean() / 1000.0), Some(hist.value_at_percentile(99.0) as f64 / 1000.0)) } else { (None, None) };
    Ok(BenchResult { ops_per_second, total_time, avg_latency_ms: avg_lat, p99_latency_ms: p99_lat, errors: total_errors, total_bytes_written, total_bytes_read })
}

fn process_read_result_chat(messages: Vec<Vec<u8>>, format: DataFormat, compressed: bool) -> bool {
    messages.into_iter().all(|bytes| process_read_result_simple(bytes, format, compressed))
}

fn process_read_result_simple(bytes: Vec<u8>, format: DataFormat, compressed: bool) -> bool {
    if bytes.is_empty() { return true; }
    let owned_bytes = if compressed {
        // Handle decompression errors
        match decode_all(bytes.as_slice()) {
            Ok(decompressed) => decompressed,
            Err(_) => return false, // Decompression failed
        }
    } else { bytes };
    match format {
        DataFormat::Rkyv => access::<Archived<BenchmarkPayload>, RkyvError>(&owned_bytes).map(|_| true).unwrap_or(false), // Uses `access` for validation
        DataFormat::Flatbuffers => flatbuffers::root::<fbs::benchmark_fbs::BenchmarkPayload>(&owned_bytes).is_ok(),
        DataFormat::Json => serde_json::from_slice::<BenchmarkPayload>(&owned_bytes).is_ok(),
        DataFormat::Bitcode => bitcode::deserialize::<BenchmarkPayload>(&owned_bytes).is_ok(),
        DataFormat::String => String::from_utf8(owned_bytes).is_ok(),
        DataFormat::Protobuf => pb::BenchmarkPayload::decode(owned_bytes.as_slice()).is_ok(),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let track_latency_globally = !cli.no_latency;

    println!("Configuration: DB={:?}, Workload={:?}, Ops={}, Concurrency={}, Format={:?}, ValueBaseSize={}B", cli.db, cli.workload, cli.num_ops, cli.concurrency, cli.format, cli.value_size);
    if cli.pubsub_only { println!("Pub/Sub mode: Publishers={}", cli.num_publishers); }
    if cli.workload == Workload::Chat { println!("Chat Sim: Chats={}, HistoryLen={}, ReadSize={}", cli.num_chats, cli.history_len, cli.read_size); }
    println!("Technical: Pipeline={}, BatchSize={}, NoLatency={}, Compression={}", cli.pipeline, cli.batch_size, cli.no_latency, if cli.compress_zstd {"zstd"} else {"none"});
    
    if cli.db != DbChoice::InMemory {
        println!("Ensure Redis ({}), Valkey ({}), and RustDb ({}) are running", cli.redis_url, cli.valkey_url, cli.rustdb_url);
    }

    if cli.num_ops == 0 { println!("Number of operations is 0, exiting."); return Ok(()); }

    let benchmarks_to_run = match (cli.write_only, cli.read_only, cli.pubsub_only) {
        (true, false, false) => vec!["WRITE"],
        (false, true, false) => vec!["READ"],
        (false, false, true) => vec!["PUBSUB"],
        (false, false, false) => vec!["WRITE", "READ"],
        _ => { eprintln!("Error: Please specify at most one of --write-only, --read-only, or --pubsub-only."); return Ok(()); }
    };

    let op_for_data_gen = if benchmarks_to_run.contains(&"PUBSUB") { 
        match cli.db {
            DbChoice::InMemory | DbChoice::RustDb => "PUBSUB_INTERNAL",
            _ => "PUBSUB",
        }
    } else { "WRITE" };
    let data = PreGeneratedData::new(&cli, op_for_data_gen);
    
    let db_choices = vec![cli.db];

    for db_choice in db_choices {
        let db_name = format!("{:?}", db_choice);
        
        let store_option: Option<Box<dyn KvStore + Send + Sync>> = if !benchmarks_to_run.contains(&"PUBSUB") || (db_choice == DbChoice::Redis || db_choice == DbChoice::Valkey) {
            Some(match db_choice {
                DbChoice::Redis => Box::new(RedisStore::new(&cli.redis_url).await?),
                DbChoice::Valkey => Box::new(RedisStore::new(&cli.valkey_url).await?),
                DbChoice::InMemory => Box::new(InMemoryStore::new()),
                DbChoice::RustDb => Box::new(RedisStore::new(&cli.rustdb_url).await?),
            })
        } else {
            None // No need for KvStore instance for internal PubSub
        };

        if benchmarks_to_run.contains(&"READ") && store_option.is_some() {
            let mut prepop_cli = cli.clone();
            prepop_cli.pipeline = true;
            prepop_cli.batch_size = cli.batch_size.max(200);
            println!("\nINFO: Pre-populating {} for READ benchmark...", db_name);
            // Pre-population always uses the store
            run_benchmark(&format!("{}-PrePop", db_name), "WRITE", store_option.as_ref().unwrap().clone(), &prepop_cli, &data, false).await?;
        }

        let mut results_table = Vec::new();
        for op_type_str_ref in &benchmarks_to_run {
            let actual_track_latency = track_latency_globally && !cli.pipeline;
            
            let result = if *op_type_str_ref == "PUBSUB" {
                match db_choice {
                    DbChoice::Redis => run_pubsub_benchmark(&db_name, &cli.redis_url, &cli, &data, !cli.no_latency).await,
                    DbChoice::Valkey => run_pubsub_benchmark(&db_name, &cli.valkey_url, &cli, &data, !cli.no_latency).await,
                    DbChoice::InMemory | DbChoice::RustDb => run_internal_pubsub_benchmark(&db_name, &cli, &data, !cli.no_latency).await,
                }
            } else {
                // Ensure store is available for non-PubSub operations
                let store_ref = store_option.as_ref().expect("KvStore should be initialized for non-PubSub benchmarks");
                let current_data = if *op_type_str_ref == "WRITE" { &data } else { &PreGeneratedData { keys: Arc::clone(&data.keys), values: None } };
                run_benchmark(&db_name, op_type_str_ref, store_ref.clone(), &cli, current_data, actual_track_latency).await
            };
            
            match result {
                Ok(res) => { results_table.push(( db_name.clone(), *op_type_str_ref, res.ops_per_second, res.total_time.as_secs_f64(), res.avg_latency_ms, res.p99_latency_ms, res.errors, res.total_bytes_written, res.total_bytes_read )); },
                Err(e) => { eprintln!("Error benchmarking {} {}: {}", db_name, op_type_str_ref, e); }
            }
        }
        
        let summary_workload = if cli.pubsub_only { "Pub/Sub".to_string() } else { format!("{:?}", cli.workload) };
        println!("\n--- Benchmark Summary (DB: {}, Format: {:?}{}, Workload: {}) ---", db_name, cli.format, if cli.compress_zstd { "+zstd" } else { "" }, summary_workload);
        let show_latency_in_table = track_latency_globally && !cli.pipeline;
        let op_col_width = 24;
        if show_latency_in_table {
            println!("{:<12} | {:<op_col_width$} | {:<14} | {:<14} | {:<14} | {:<12} | {:<12} | {:<8}", "Database", "Op Type", "Ops/sec", "Speed (MB/s)", "Total Traffic", "Avg Lat(ms)", "P99 Lat(ms)", "Errors");
            println!("{:-<13}|{:-<op_col_width$}|{:-<16}|{:-<16}|{:-<16}|{:-<14}|{:-<14}|{:-<10}", "-", "-", "-", "-", "-", "-", "-", "-");
        } else {
            println!("{:<12} | {:<op_col_width$} | {:<14} | {:<14} | {:<14} | {:<8}", "Database", "Op Type", "Ops/sec", "Speed (MB/s)", "Total Traffic", "Errors");
            println!("{:-<13}|{:-<op_col_width$}|{:-<16}|{:-<16}|{:-<16}|{:-<10}", "-", "-", "-", "-", "-", "-");
        }

        for (db, op, ops_sec, time_s, avg_lat, p99_lat, errors, bytes_written, bytes_read) in results_table.iter() {
            let op_name = if *op == "READ" {
                let zc_suffix = match cli.format {
                    DataFormat::Rkyv | DataFormat::Flatbuffers => " (Zero-Copy)",
                    _ => "",
                };
                format!("READ ({:?}){}", cli.workload, zc_suffix)
            } else if *op == "PUBSUB" {
                let suffix = match db_choice {
                    DbChoice::InMemory | DbChoice::RustDb => " (Internal)".to_string(),
                    _ => "".to_string(),
                };
                format!("PUBSUB ({} Pubs){}", cli.num_publishers, suffix)
            } else {
                format!("WRITE ({:?})", cli.workload)
            };

            let total_traffic_bytes = *bytes_written + *bytes_read;
            let traffic_speed_mb_s = if *time_s > 0.0 { (total_traffic_bytes as f64 / *time_s) / 1_000_000.0 } else { 0.0 };
            let total_traffic_str = Byte::from(total_traffic_bytes).get_appropriate_unit(UnitType::Binary).to_string();

            if show_latency_in_table {
                println!("{:<12} | {:<op_col_width$} | {:<14.2} | {:<14.2} | {:<14} | {:<12.3} | {:<12.3} | {:<8}", db, op_name, ops_sec, traffic_speed_mb_s, total_traffic_str, avg_lat.unwrap_or(0.0), p99_lat.unwrap_or(0.0), errors);
            } else {
                println!("{:<12} | {:<op_col_width$} | {:<14.2} | {:<14.2} | {:<14} | {:<8}", db, op_name, ops_sec, traffic_speed_mb_s, total_traffic_str, errors);
            }
        }
    }

    Ok(())
}