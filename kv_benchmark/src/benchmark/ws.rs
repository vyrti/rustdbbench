use crate::benchmark::data::{BenchResult, PreGeneratedData, WorkerResult};
use crate::benchmark::runner::process_read_result_simple;
use crate::cli::Cli;
use anyhow::{anyhow, Result};
use futures::{stream::StreamExt, SinkExt};
use indicatif::{ProgressBar, ProgressStyle};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message;

pub async fn run_ws_benchmark(db_name: &str, ws_url: &str, cli: &Cli, data: &PreGeneratedData, track_latency: bool) -> Result<BenchResult> {
    let num_publishers = cli.num_publishers;
    let num_subscribers = cli.concurrency.saturating_sub(num_publishers);
    if num_subscribers == 0 {
        return Err(anyhow!("WebSocket benchmark requires at least one subscriber."));
    }
    if num_publishers == 0 {
        return Err(anyhow!("WebSocket benchmark requires at least one publisher."));
    }
    println!("\nBenchmarking {} WebSocket ({} pubs, {} subs, {} ops total)...", db_name, num_publishers, num_subscribers, cli.num_ops);

    let pb = ProgressBar::new(cli.num_ops as u64);
    pb.set_style(ProgressStyle::default_bar().template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}").unwrap().progress_chars("#>-"));
    pb.set_message(format!("{} WebSocket", db_name));
    let pb_arc = Arc::new(pb);

    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
    let ready_barrier = Arc::new(tokio::sync::Barrier::new(num_subscribers + 1));
    let received_msg_count = Arc::new(AtomicUsize::new(0));
    let received_bytes_count = Arc::new(AtomicU64::new(0));

    let mut sub_tasks = Vec::with_capacity(num_subscribers);
    for _ in 0..num_subscribers {
        let barrier = ready_barrier.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        let msg_count = received_msg_count.clone();
        let bytes_count = received_bytes_count.clone();
        let url = ws_url.to_string();
        let cli_clone = cli.clone();

        sub_tasks.push(tokio::spawn(async move {
            let (ws_stream, _) = connect_async(&url).await.expect("Subscriber failed to connect");
            let (_, mut read) = ws_stream.split();
            barrier.wait().await;

            loop {
                tokio::select! {
                    biased;
                    _ = shutdown_rx.recv() => break,
                    Some(msg) = read.next() => {
                        if let Ok(Message::Binary(bin)) = msg {
                            // Perform deserialization check, including rkyv zero-copy
                            process_read_result_simple(&bin, cli_clone.format, cli_clone.compress_zstd);
                            msg_count.fetch_add(1, Ordering::Relaxed);
                            bytes_count.fetch_add(bin.len() as u64, Ordering::Relaxed);
                        } else if let Ok(Message::Close(_)) = msg {
                            break;
                        }
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
        let url = ws_url.to_string();
        let values_ref = data.values.as_ref().unwrap().clone();
        let progress_bar_clone = pb_arc.clone();
        let worker_start_idx = worker_id * ops_per_publisher;
        let worker_end_idx = ((worker_id + 1) * ops_per_publisher).min(cli.num_ops);
        if worker_start_idx >= worker_end_idx {
            continue;
        }

        pub_tasks.push(tokio::spawn(async move {
            let (ws_stream, _) = connect_async(&url).await.expect("Publisher failed to connect");
            let (mut write, read) = ws_stream.split();

            let read_handle = tokio::spawn(async move {
                let mut read_stream = read;
                while read_stream.next().await.is_some() {}
            });

            let mut hist = if track_latency { Some(hdrhistogram::Histogram::<u64>::new(3).unwrap()) } else { None };
            let mut local_errors = 0;
            let mut local_ops_done = 0;
            let mut local_bytes_written = 0;

            for i in worker_start_idx..worker_end_idx {
                let value = &values_ref[i];
                let msg = Message::Binary(value.clone().into());

                let op_start_time = if track_latency { Some(Instant::now()) } else { None };
                if write.send(msg).await.is_ok() {
                    local_ops_done += 1;
                    local_bytes_written += value.len() as u64;
                } else {
                    local_errors += 1;
                }
                if let (Some(st), Some(h)) = (op_start_time, hist.as_mut()) {
                    h.record(st.elapsed().as_micros() as u64).unwrap();
                }
            }
            progress_bar_clone.inc((worker_end_idx - worker_start_idx) as u64);
            
            let _ = write.close().await;
            read_handle.await.ok();

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
    let mut total_ops_completed = 0;
    let mut total_bytes_written = 0;

    for result in publisher_results {
        if let Ok(wr) = result {
            if let (Some(fh), Some(wh)) = (final_histogram.as_mut(), wr.histogram.as_ref()) {
                fh.add(wh).unwrap();
            }
            total_errors += wr.errors;
            total_ops_completed += wr.ops_done;
            total_bytes_written += wr.bytes_written;
        } else {
            eprintln!("Publisher worker task panicked.");
            total_errors += ops_per_publisher;
            total_ops_completed += ops_per_publisher;
        }
    }

    let total_bytes_read_raw = received_bytes_count.load(Ordering::SeqCst);
    let successful_ops = total_ops_completed.saturating_sub(total_errors);
    let ops_per_second = if total_time.as_secs_f64() > 0.0 { successful_ops as f64 / total_time.as_secs_f64() } else { 0.0 };
    let (avg_lat, p99_lat) = if let Some(hist) = final_histogram.as_ref() { (Some(hist.mean() / 1000.0), Some(hist.value_at_percentile(99.0) as f64 / 1000.0)) } else { (None, None) };

    Ok(BenchResult { ops_per_second, total_time, avg_latency_ms: avg_lat, p99_latency_ms: p99_lat, errors: total_errors, total_ops_completed, total_bytes_written, total_bytes_read: total_bytes_read_raw })
}