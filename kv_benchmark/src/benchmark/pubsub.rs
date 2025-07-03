use crate::benchmark::data::{BenchResult, PreGeneratedData, WorkerResult};
use crate::benchmark::runner::process_read_result_simple;
use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use futures::stream::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use rand::{prelude::*, rngs::StdRng};
use redis::AsyncCommands;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;

const DEFAULT_KEY_PREFIX: &str = "bench";

// New struct to encapsulate the internal Pub/Sub logic
#[derive(Clone)]
pub struct InternalPubSub {
    // DashMap to hold broadcast senders for each topic/channel
    topic_senders: Arc<DashMap<String, broadcast::Sender<Arc<Bytes>>>>,
    // Channel size for broadcast senders
    channel_size: usize,
}

impl InternalPubSub {
    pub fn new(channel_size: usize) -> Self {
        Self {
            topic_senders: Arc::new(DashMap::new()),
            channel_size,
        }
    }

    // Publishes a message to a given topic. Message is Arc<Bytes> for zero-copy.
    pub fn publish(&self, topic: String, message: Arc<Bytes>) {
        let sender = self.topic_senders
            .entry(topic)
            .or_insert_with(|| broadcast::channel(self.channel_size).0); // Get or create sender
        let _ = sender.send(message); // Ignore send errors (e.g., no receivers)
    }

    // Subscribes to a given topic. Returns a broadcast receiver.
    pub fn subscribe(&self, topic: String) -> broadcast::Receiver<Arc<Bytes>> {
        // Get or create sender, then subscribe
        let sender = self.topic_senders
            .entry(topic)
            .or_insert_with(|| broadcast::channel(self.channel_size).0)
            .clone();
        sender.subscribe()
    }
}

pub async fn run_pubsub_benchmark(db_name: &str, db_url_slice: &str, cli: &crate::cli::Cli, data: &PreGeneratedData, track_latency: bool) -> Result<BenchResult> {
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
            WorkerResult { histogram: hist, errors: local_errors, ops_done: local_ops_done, bytes_written: local_bytes_written, bytes_read: 0, active_ws_connections: None, lagged_messages: 0 }
        }));
    }
    let publisher_results = futures::future::join_all(pub_tasks).await;
    let total_time = start_time.elapsed();
    let _ = shutdown_tx.send(());
    pb_arc.finish_with_message("Done");
    let _ = futures::future::join_all(sub_tasks).await;
    let mut final_histogram = if track_latency { Some(hdrhistogram::Histogram::<u64>::new(3).unwrap()) } else { None };
    let mut total_errors = 0;
    let mut total_ops_completed = 0;
    let mut total_bytes_written = 0;
    for result in publisher_results {
        if let Ok(wr) = result {
            if let (Some(fh), Some(wh)) = (final_histogram.as_mut(), wr.histogram.as_ref()) { fh.add(wh).unwrap(); }
            total_errors += wr.errors;
            total_ops_completed += wr.ops_done;
            total_bytes_written += wr.bytes_written;
        } else {
            eprintln!("Publisher worker task panicked.");
            total_errors += ops_per_publisher;
        }
    }
    let total_bytes_read_raw = received_bytes_count.load(Ordering::SeqCst);

    let successful_ops = total_ops_completed.saturating_sub(total_errors);
    let ops_per_second = if total_time.as_secs_f64() > 0.0 { successful_ops as f64 / total_time.as_secs_f64() } else { 0.0 };
    let (avg_lat, p99_lat) = if let Some(hist) = final_histogram.as_ref() { (Some(hist.mean() / 1000.0), Some(hist.value_at_percentile(99.0) as f64 / 1000.0)) } else { (None, None) };
    Ok(BenchResult { ops_per_second, total_time, avg_latency_ms: avg_lat, p99_latency_ms: p99_lat, errors: total_errors, total_ops_completed, total_bytes_written, total_bytes_read: total_bytes_read_raw, total_lagged_messages: 0 })
}

pub async fn run_internal_pubsub_benchmark(db_name: &str, cli: &crate::cli::Cli, data: &PreGeneratedData, track_latency: bool) -> Result<BenchResult> {
    let num_publishers = cli.num_publishers;
    let num_subscribers = cli.concurrency.saturating_sub(num_publishers);
    if num_subscribers == 0 { return Err(anyhow::anyhow!("Pub/Sub benchmark requires at least one subscriber.")); }
    if num_publishers == 0 { return Err(anyhow::anyhow!("Pub/Sub benchmark requires at least one publisher.")); }
    println!("\nBenchmarking {} ZERO-COPY INTERNAL BROADCAST PUBSUB ({} pubs, {} subs, {} ops total)...", db_name, num_publishers, num_subscribers, cli.num_ops);

    let internal_pubsub = InternalPubSub::new(1024); // Increased channel size for high throughput
    let pb = ProgressBar::new(cli.num_ops as u64);
    pb.set_style(ProgressStyle::default_bar().template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}").unwrap().progress_chars("#>-"));
    pb.set_message(format!("{} Zero-Copy PUBSUB", db_name));
    let pb_arc = Arc::new(pb);

    // Atomics for tracking subscriber results
    let received_msg_count = Arc::new(AtomicUsize::new(0));
    let received_bytes_count = Arc::new(AtomicU64::new(0));
    let lagged_msg_count = Arc::new(AtomicUsize::new(0)); // For tracking lagged messages

    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    let ready_barrier = Arc::new(tokio::sync::Barrier::new(num_subscribers + num_publishers));
    let mut sub_tasks = Vec::with_capacity(num_subscribers);

    const SINGLE_TOPIC: &str = "benchmark_broadcast_channel";

    for _ in 0..num_subscribers {
        let internal_pubsub_clone = internal_pubsub.clone();
        let msg_count = received_msg_count.clone();
        let bytes_count = received_bytes_count.clone();
        let lag_count = lagged_msg_count.clone();
        let barrier = ready_barrier.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        let cli_format = cli.format;
        let cli_compress_zstd = cli.compress_zstd;

        sub_tasks.push(tokio::spawn(async move {
            // REFACTORED: Subscribe to one topic only.
            let mut receiver = internal_pubsub_clone.subscribe(SINGLE_TOPIC.to_string());
            barrier.wait().await;

            // REFACTORED: Simplified receive loop.
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown_rx.recv() => break,
                    recv_res = receiver.recv() => {
                        match recv_res {
                            Ok(msg_arc) => {
                                msg_count.fetch_add(1, Ordering::Relaxed);
                                bytes_count.fetch_add(msg_arc.len() as u64, Ordering::Relaxed);
                                // We can still process the read to simulate workload, but it's optional.
                                let _ = process_read_result_simple(&msg_arc, cli_format, cli_compress_zstd);
                            },
                            Err(RecvError::Lagged(n)) => {
                                // This is a critical metric. It means this subscriber is too slow.
                                lag_count.fetch_add(n as usize, Ordering::Relaxed);
                            },
                            Err(RecvError::Closed) => break,
                        }
                    },
                }
            }
        }));
    }

    let mut pub_tasks = Vec::with_capacity(num_publishers);
    let ops_per_publisher = (cli.num_ops + num_publishers - 1) / num_publishers;
    for worker_id in 0..num_publishers {
        let internal_pubsub_clone = internal_pubsub.clone();
        // REFACTORED: Use pre-generated values for fair comparison.
        let values_ref = data.values.as_ref().expect("Values must be pre-generated for Pub/Sub benchmark").clone();
        let progress_bar_clone = pb_arc.clone();
        let barrier = ready_barrier.clone();
        let worker_start_idx = worker_id * ops_per_publisher;
        let worker_end_idx = ((worker_id + 1) * ops_per_publisher).min(cli.num_ops);
        if worker_start_idx >= worker_end_idx { continue; }

        pub_tasks.push(tokio::spawn(async move {
            let mut hist = if track_latency { Some(hdrhistogram::Histogram::<u64>::new(3).unwrap()) } else { None };
            let mut local_ops_done = 0;
            let mut local_bytes_written = 0;

            barrier.wait().await;

            for i in worker_start_idx..worker_end_idx {
                let value_vec = &values_ref[i];
                // Create the zero-copy Arc<Bytes> from the pre-serialized Vec<u8>
                let value_bytes: Arc<Bytes> = Arc::new(Bytes::from(value_vec.clone()));

                let op_start_time = if track_latency { Some(Instant::now()) } else { None };

                // Publish to the single, shared topic
                internal_pubsub_clone.publish(SINGLE_TOPIC.to_string(), value_bytes.clone());

                local_ops_done += 1;
                local_bytes_written += value_bytes.len() as u64;
                if let (Some(st), Some(h)) = (op_start_time, hist.as_mut()) { h.record(st.elapsed().as_micros() as u64).unwrap(); }
            }
            progress_bar_clone.inc((worker_end_idx - worker_start_idx) as u64);
            // Lagged messages are tracked by subscribers, so publisher result is 0.
            WorkerResult { histogram: hist, errors: 0, ops_done: local_ops_done, bytes_written: local_bytes_written, bytes_read: 0, active_ws_connections: None, lagged_messages: 0 }
        }));
    }

    let start_time = Instant::now(); // Start timing after barrier is passed by publishers
    let publisher_results = futures::future::join_all(pub_tasks).await;
    let total_time_publishing = start_time.elapsed();

    // Now that publishing is done, signal subscribers to shut down.
    let _ = shutdown_tx.send(());
    pb_arc.finish_with_message("Done");
    let _ = futures::future::join_all(sub_tasks).await;

    let mut final_histogram = if track_latency { Some(hdrhistogram::Histogram::<u64>::new(3).unwrap()) } else { None };
    let mut total_errors = 0;
    let mut total_ops_completed = 0;
    let mut total_bytes_written = 0;
    for result in publisher_results {
        if let Ok(wr) = result {
            if let (Some(fh), Some(wh)) = (final_histogram.as_mut(), wr.histogram.as_ref()) { fh.add(wh).unwrap(); }
            total_errors += wr.errors;
            total_ops_completed += wr.ops_done;
            total_bytes_written += wr.bytes_written;
        } else {
            eprintln!("Publisher worker task panicked.");
            total_errors += ops_per_publisher;
        }
    }
    let total_bytes_read_raw = received_bytes_count.load(Ordering::SeqCst);
    let total_lagged_messages = lagged_msg_count.load(Ordering::SeqCst);

    let successful_ops = total_ops_completed.saturating_sub(total_errors);
    let ops_per_second = if total_time_publishing.as_secs_f64() > 0.0 { successful_ops as f64 / total_time_publishing.as_secs_f64() } else { 0.0 };
    let (avg_lat, p99_lat) = if let Some(hist) = final_histogram.as_ref() { (Some(hist.mean() / 1000.0), Some(hist.value_at_percentile(99.0) as f64 / 1000.0)) } else { (None, None) };

    Ok(BenchResult {
        ops_per_second,
        total_time: total_time_publishing,
        avg_latency_ms: avg_lat,
        p99_latency_ms: p99_lat,
        errors: total_errors,
        total_ops_completed,
        total_bytes_written,
        total_bytes_read: total_bytes_read_raw,
        total_lagged_messages,
    })
}