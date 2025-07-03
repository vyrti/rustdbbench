use crate::benchmark::data::{BenchResult, PreGeneratedData, WorkerResult};
use anyhow::Result;
use futures::stream::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use rand::{prelude::*, rngs::StdRng};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

const NATS_BENCH_SUBJECT_PREFIX: &str = "bench";

pub async fn run_nats_pubsub_benchmark(db_name: &str, nats_url: &str, cli: &crate::cli::Cli, data: &PreGeneratedData, track_latency: bool) -> Result<BenchResult> {
    let num_publishers = cli.num_publishers;
    let num_subscribers = cli.concurrency.saturating_sub(num_publishers);
    if num_subscribers == 0 { return Err(anyhow::anyhow!("Pub/Sub benchmark requires at least one subscriber.")); }
    if num_publishers == 0 { return Err(anyhow::anyhow!("Pub/Sub benchmark requires at least one publisher.")); }
    println!("\nBenchmarking {} PUBSUB ({} pubs, {} subs, {} ops total)...", db_name, num_publishers, num_subscribers, cli.num_ops);
    
    // Connect to NATS server using async-nats
    let nats_conn = async_nats::connect(nats_url).await?;
    println!("Connected to NATS server at {}", nats_url);

    let pb = ProgressBar::new(cli.num_ops as u64);
    pb.set_style(ProgressStyle::default_bar().template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}").unwrap().progress_chars("#>-"));
    pb.set_message(format!("{} PUBSUB", db_name));
    let pb_arc = Arc::new(pb);

    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
    let ready_barrier = Arc::new(tokio::sync::Barrier::new(num_subscribers + num_publishers));
    
    // Atomics for tracking results
    let received_msg_count = Arc::new(AtomicUsize::new(0));
    let received_bytes_count = Arc::new(AtomicU64::new(0));
    
    let mut sub_tasks = Vec::with_capacity(num_subscribers);
    for _ in 0..num_subscribers {
        let conn = nats_conn.clone();
        let barrier = ready_barrier.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        let msg_count = received_msg_count.clone();
        let bytes_count = received_bytes_count.clone();
        
        // Use a wildcard subscription, the correct pattern for this workload.
        let subject_wildcard = format!("{}.*", NATS_BENCH_SUBJECT_PREFIX);

        sub_tasks.push(tokio::spawn(async move {
            let mut sub = conn.subscribe(subject_wildcard).await.unwrap();
            barrier.wait().await;
            
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown_rx.recv() => break,
                    Some(msg) = sub.next() => {
                        msg_count.fetch_add(1, Ordering::Relaxed);
                        bytes_count.fetch_add(msg.payload.len() as u64, Ordering::Relaxed);
                    },
                    else => break,
                }
            }
        }));
    }

    let mut pub_tasks = Vec::with_capacity(num_publishers);
    let ops_per_publisher = (cli.num_ops + num_publishers - 1) / num_publishers;
    for worker_id in 0..num_publishers {
        let conn = nats_conn.clone();
        let keys_ref = Arc::clone(&data.keys);
        let values_ref = data.values.as_ref().unwrap().clone();
        let progress_bar_clone = pb_arc.clone();
        let barrier = ready_barrier.clone();
        let worker_start_idx = worker_id * ops_per_publisher;
        let worker_end_idx = ((worker_id + 1) * ops_per_publisher).min(cli.num_ops);
        if worker_start_idx >= worker_end_idx { continue; }

        pub_tasks.push(tokio::spawn(async move {
            let mut hist = if track_latency { Some(hdrhistogram::Histogram::<u64>::new(3).unwrap()) } else { None };
            let mut local_errors = 0;
            let mut local_ops_done = 0;
            let mut local_bytes_written = 0;
            let mut rng = StdRng::from_entropy();
            barrier.wait().await;

            for i in worker_start_idx..worker_end_idx {
                // In NATS, the key is called a "subject".
                let subject = &keys_ref[rng.gen_range(0..keys_ref.len())];
                let value = &values_ref[i]; // This is &Vec<u8>
                let op_start_time = if track_latency { Some(Instant::now()) } else { None };
                
                // publish() in async-nats takes an Into<Payload>, and Vec<u8> can be converted into it.
                // We clone the vector to perform the conversion.
                if conn.publish(subject.clone(), value.clone().into()).await.is_ok() {
                    local_ops_done += 1;
                    local_bytes_written += value.len() as u64;
                } else {
                    local_errors += 1;
                }
                
                if let (Some(st), Some(h)) = (op_start_time, hist.as_mut()) { h.record(st.elapsed().as_micros() as u64).unwrap(); }
            }
            conn.flush().await.unwrap_or_default(); // Ensure all buffered messages are sent.
            progress_bar_clone.inc((worker_end_idx - worker_start_idx) as u64);
            WorkerResult { histogram: hist, errors: local_errors, ops_done: local_ops_done, bytes_written: local_bytes_written, bytes_read: 0, active_ws_connections: None, lagged_messages: 0 }
        }));
    }

    let start_time = Instant::now();
    let publisher_results = futures::future::join_all(pub_tasks).await;
    let total_time = start_time.elapsed();
    
    // Shutdown subscribers
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