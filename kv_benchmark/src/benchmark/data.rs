use crate::cli::{Cli, DataFormat, Workload};
use crate::{fbs, pb};
use prost::Message as ProtobufMessage;
use rand::{
    distributions::{Alphanumeric, Distribution},
    prelude::*,
    rngs::StdRng,
};
use rkyv::{
    rancor::Error as RkyvError, to_bytes, Archive, Deserialize as RkyvDeserialize,
    Serialize as RkyvSerialize,
};
use serde::{Deserialize as SerdeDeserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use zstd::stream::encode_all;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

#[derive(Serialize, SerdeDeserialize, Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone)]
pub struct BenchmarkPayload {
    pub data: String,
    pub timestamp: u64,
    pub id: usize,
}

pub struct PreGeneratedData {
    pub keys: Arc<Vec<String>>,
    pub values: Option<Arc<Vec<Vec<u8>>>>,
}

impl PreGeneratedData {
    pub fn new(cli: &Cli, op_type_for_values: &str) -> Self {
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

pub struct BenchResult {
    pub ops_per_second: f64,
    pub total_time: Duration,
    pub avg_latency_ms: Option<f64>,
    pub p99_latency_ms: Option<f64>,
    pub errors: usize,
    pub total_ops_completed: usize,
    pub total_bytes_written: u64,
    pub total_bytes_read: u64,
}

pub struct WorkerResult {
    pub histogram: Option<hdrhistogram::Histogram<u64>>,
    pub errors: usize,
    pub ops_done: usize,
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub active_ws_connections: Option<Vec<WebSocketStream<MaybeTlsStream<TcpStream>>>>,
}

pub fn generate_random_string(len: usize, rng: &mut impl Rng) -> String {
    let mut s = String::with_capacity(len);
    (0..len).for_each(|_| s.push(Alphanumeric.sample(rng) as char));
    s
}