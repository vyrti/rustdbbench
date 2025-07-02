use clap::{Parser, ValueEnum};

pub const DEFAULT_BATCH_SIZE: usize = 50;

#[derive(ValueEnum, Clone, Debug, Copy, PartialEq, Eq)]
pub enum DataFormat {
    String, Json, Bitcode, Protobuf, Rkyv, Flatbuffers,
}

#[derive(ValueEnum, Clone, Debug, Copy, PartialEq, Eq)]
pub enum Workload {
    Chat, Simple,
}

#[derive(ValueEnum, Clone, Debug, Copy, PartialEq, Eq)]
pub enum DbChoice {
    Redis, Valkey, InMemory, RustDb, WebSocket, WebSocketChat, // Added WebSocketChat
}

#[derive(Parser, Debug, Clone)]
#[clap(author, version, about, long_about = None)]
pub struct Cli {
    #[clap(long, value_enum, default_value_t = DbChoice::Redis, help = "Database backend to use.")]
    pub db: DbChoice,
    #[clap(short, long, default_value = "redis://127.0.0.1:6379")]
    pub redis_url: String,
    #[clap(short, long, default_value = "redis://127.0.0.1:6380")]
    pub valkey_url: String,
    #[clap(long, default_value = "redis://127.0.0.1:7878", help="URL for the custom Rust Redis server.")]
    pub rustdb_url: String,
    #[clap(long, default_value = "ws://127.0.0.1:3000/ws", help="URL for the WebSocket server.")]
    pub ws_url: String,
    #[clap(long, default_value = "127.0.0.2", help = "Base IP address for multi-IP client mode.")]
    pub ws_multi_ip_base: String,
    #[clap(long, default_value_t = 200, help = "Number of IP aliases to use in multi-IP mode.")]
    pub ws_multi_ip_count: u32,
    #[clap(long, help = "Enable multi-IP client mode for WS benchmark to overcome port limits.")]
    pub ws_multi_ip: bool,
    #[clap(long, value_enum, default_value_t = Workload::Chat, help = "The type of benchmark workload to run.")]
    pub workload: Workload,
    #[clap(short = 'o', long, default_value_t = 100_000, help="Total number of operations to perform.")]
    pub num_ops: usize,
    #[clap(short, long, default_value = "50", help="Number of concurrent client threads.")]
    pub concurrency: usize,
    #[clap(long, default_value_t = 128, help="Base size in bytes for the data part of a message payload.")]
    pub value_size: usize,
    #[clap(long, value_enum, default_value_t = DataFormat::String, help = "Data serialization format for values")]
    pub format: DataFormat,
    #[clap(long, default_value_t = 1000, help="For Chat workload: number of simulated chat rooms.")]
    pub num_chats: usize,
    #[clap(long, default_value_t = 100, help="For Chat workload: number of messages to keep in chat history (for LTRIM).")]
    pub history_len: usize,
    #[clap(long, default_value_t = 50, help="For Chat workload: number of messages to fetch in a single READ operation (for LRANGE).")]
    pub read_size: usize,
    #[clap(long, help = "Run only write benchmarks")]
    pub write_only: bool,
    #[clap(long, help = "Run only read benchmarks")]
    pub read_only: bool,
    #[clap(long, help = "Run only Pub/Sub benchmarks")]
    pub pubsub_only: bool,
    #[clap(long, help = "For Pub/Sub: number of publisher clients", default_value_t = 1)]
    pub num_publishers: usize,
    #[clap(long, help = "Use explicit pipelining for commands")]
    pub pipeline: bool,
    #[clap(long, default_value_t = DEFAULT_BATCH_SIZE, help = "Batch size for pipelined commands")]
    pub batch_size: usize,
    #[clap(long, help = "Skip latency tracking (useful for max throughput tests or pipelined mode)")]
    pub no_latency: bool,
    #[clap(long, help = "Enable zstd compression for values")]
    pub compress_zstd: bool,
    #[clap(long, help = "Run WebSocket server instead of benchmark.")]
    pub run_ws_server: bool,
}