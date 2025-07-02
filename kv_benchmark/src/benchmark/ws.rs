use crate::benchmark::data::{BenchResult, PreGeneratedData, WorkerResult};
use crate::cli::Cli;
use anyhow::{anyhow, Result};
// use futures::{stream::StreamExt, SinkExt};
use indicatif::{ProgressBar, ProgressStyle};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio_tungstenite::connect_async;
// use tokio_tungstenite::tungstenite::protocol::Message; // Commented out as requested
use tokio_tungstenite::WebSocketStream;
use tokio::net::TcpStream;
use tokio_tungstenite::MaybeTlsStream; // Required for the correct stream type

// COMMENTED OUT: Previous Pub/Sub benchmark logic.
/*
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
*/

// NEW CODE: WebSocket Connection Capacity Benchmark
pub async fn run_ws_benchmark(db_name: &str, ws_url: &str, cli: &Cli, _data: &PreGeneratedData, track_latency: bool) -> Result<BenchResult> {
    // For connection capacity, cli.num_ops will be interpreted as the target number of connections to establish.
    // cli.concurrency will be the number of concurrent tasks opening connections.
    if cli.num_ops == 0 {
        return Err(anyhow!("Target number of connections (num-ops) must be greater than 0."));
    }
    if cli.concurrency == 0 {
        return Err(anyhow!("Concurrency (number of client threads) must be greater than 0."));
    }

    println!("\nBenchmarking {} WebSocket Connection Capacity (Target: {} connections, {} concurrent clients)...", db_name, cli.num_ops, cli.concurrency);

    let pb = ProgressBar::new(cli.num_ops as u64);
    pb.set_style(ProgressStyle::default_bar().template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}").unwrap().progress_chars("#>-"));
    pb.set_message(format!("{} WS Connections", db_name));
    let pb_arc = Arc::new(pb);

    // Atomic counters for connections
    let established_connections_total = Arc::new(AtomicUsize::new(0));
    let failed_connections_total = Arc::new(AtomicUsize::new(0));

    let mut client_tasks = Vec::with_capacity(cli.concurrency);
    let connections_per_worker = (cli.num_ops + cli.concurrency - 1) / cli.concurrency;

    let total_start_time = Instant::now();

    for worker_id in 0..cli.concurrency {
        let url = ws_url.to_string();
        let progress_bar_clone = pb_arc.clone();
        let established_connections_clone = established_connections_total.clone();
        let failed_connections_clone = failed_connections_total.clone();
        let track_latency_clone = track_latency;

        client_tasks.push(tokio::spawn(async move {
            let mut hist = if track_latency_clone { Some(hdrhistogram::Histogram::<u64>::new(3).unwrap()) } else { None };
            let mut local_established_count = 0;
            let mut local_failed_count = 0;
            
            // This vector will hold the active WebSocketStream objects, preventing them from being dropped
            // and thus keeping the connections alive for the duration of this worker task.
            let mut active_connections: Vec<WebSocketStream<MaybeTlsStream<TcpStream>>> = Vec::with_capacity(connections_per_worker);

            for i in 0..connections_per_worker {
                let op_start_time = if track_latency_clone { Some(Instant::now()) } else { None };
                match tokio::time::timeout(std::time::Duration::from_secs(10), connect_async(&url)).await {
                    Ok(Ok((ws_stream, _))) => {
                        local_established_count += 1;
                        established_connections_clone.fetch_add(1, Ordering::Relaxed);
                        // Store the stream to keep the connection open
                        active_connections.push(ws_stream);

                        // If tracking latency, record after successful connection
                        if let (Some(st), Some(h)) = (op_start_time, hist.as_mut()) {
                            h.record(st.elapsed().as_micros() as u64).unwrap();
                        }
                    },
                    Ok(Err(e)) => {
                        local_failed_count += 1;
                        failed_connections_clone.fetch_add(1, Ordering::Relaxed);
                        // Limit error logs to avoid spam if many connections fail
                        if local_failed_count <= 5 || i % 1000 == 0 {
                             eprintln!("Worker {}: Failed to connect (attempt {}): {}", worker_id, i, e);
                        } else if local_failed_count == 6 {
                             eprintln!("Worker {}: ... (further connection errors suppressed for this worker)", worker_id);
                        }
                    },
                    Err(_) => { // Timeout error
                        local_failed_count += 1;
                        failed_connections_clone.fetch_add(1, Ordering::Relaxed);
                        if local_failed_count <= 5 || i % 1000 == 0 {
                            eprintln!("Worker {}: Connection attempt (attempt {}) timed out.", worker_id, i);
                        } else if local_failed_count == 6 {
                             eprintln!("Worker {}: ... (further connection timeouts suppressed for this worker)", worker_id);
                        }
                    }
                }
                progress_bar_clone.inc(1);
            }

            // The connections held in `active_connections` will stay open
            // until this worker task finishes and the `active_connections` vector is dropped.
            // For sustained testing beyond just establishment, these streams would need to be
            // moved out of the worker task and handled by long-lived background tasks
            // or a shared pool. For this capacity benchmark, this is sufficient.

            WorkerResult {
                histogram: hist,
                errors: local_failed_count,
                ops_done: local_established_count, // connections established by this worker
                bytes_written: 0, // Minimal handshake bytes, not tracked as application data
                bytes_read: 0,    // Minimal handshake bytes, not tracked as application data
            }
        }));
    }

    let worker_results = futures::future::join_all(client_tasks).await;
    let total_time_taken_to_establish = total_start_time.elapsed();
    pb_arc.finish_with_message("Done");

    let mut final_histogram = if track_latency { Some(hdrhistogram::Histogram::<u64>::new(3).unwrap()) } else { None };
    // These now use atomics directly for aggregate totals, which are more robust.
    let total_connections_established = established_connections_total.load(Ordering::SeqCst);
    let total_connections_failed = failed_connections_total.load(Ordering::SeqCst);

    for result in worker_results {
        if let Ok(wr) = result {
            if let (Some(fh), Some(wh)) = (final_histogram.as_mut(), wr.histogram.as_ref()) {
                fh.add(wh).unwrap();
            }
            // wr.errors and wr.ops_done are already accumulated by atomics, no need to sum again here.
        } else {
            eprintln!("A worker task panicked. This may affect total counts reported.");
        }
    }

    let connections_per_second = if total_time_taken_to_establish.as_secs_f64() > 0.0 {
        total_connections_established as f64 / total_time_taken_to_establish.as_secs_f64()
    } else {
        0.0
    };

    let (avg_lat, p99_lat) = if let Some(hist) = final_histogram.as_ref() {
        (Some(hist.mean() / 1000.0), Some(hist.value_at_percentile(99.0) as f64 / 1000.0))
    } else {
        (None, None)
    };

    println!("\n--- Connection Capacity Benchmark Results ---");
    println!("Target connections: {}", cli.num_ops);
    println!("Successfully established: {}", total_connections_established);
    println!("Failed to establish: {}", total_connections_failed);
    println!("Total time to establish: {:?}", total_time_taken_to_establish);
    println!("Establishment Rate: {:.2} connections/sec", connections_per_second);

    // BenchResult fields mapping for this test:
    // ops_per_second: connections_per_second (establishment rate)
    // total_time: total_time_taken_to_establish
    // avg_latency_ms: avg connection establishment latency
    // p99_latency_ms: p99 connection establishment latency
    // errors: total_connections_failed
    // total_ops_completed: total_connections_established
    // total_bytes_written/read: 0 (minimal handshake bytes are not tracked as application data traffic)

    Ok(BenchResult {
        ops_per_second: connections_per_second,
        total_time: total_time_taken_to_establish,
        avg_latency_ms: avg_lat,
        p99_latency_ms: p99_lat,
        errors: total_connections_failed,
        total_ops_completed: total_connections_established,
        total_bytes_written: 0,
        total_bytes_read: 0,
    })
}