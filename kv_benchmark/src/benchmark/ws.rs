use crate::benchmark::data::{BenchResult, BenchmarkPayload, PreGeneratedData, WorkerResult};
use crate::benchmark::data::generate_random_string;
use crate::benchmark::runner::process_read_result_simple;
use crate::cli::Cli;
use anyhow::{anyhow, Result};
use futures::{stream::StreamExt, SinkExt};
use indicatif::{ProgressBar, ProgressStyle};
use std::net::{Ipv4Addr, SocketAddr};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::{TcpSocket};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{client_async, MaybeTlsStream};
use rand::{prelude::*, rngs::StdRng};
use rkyv::{rancor::Error as RkyvError, to_bytes};

pub async fn run_ws_benchmark(db_name: &str, ws_url: &str, cli: &Cli, _data: &PreGeneratedData, track_latency: bool) -> Result<BenchResult> {
    if cli.num_ops == 0 {
        return Err(anyhow!("Total operations (num-ops) must be greater than 0."));
    }
    if cli.concurrency == 0 {
        return Err(anyhow!("Concurrency (number of client threads) must be greater than 0."));
    }

    // --- Multi-IP Logic ---
    let ip_addresses = if cli.ws_multi_ip {
        println!("INFO: WebSocket benchmark running in multi-IP mode.");
        let base_ip = Ipv4Addr::from_str(&cli.ws_multi_ip_base)
            .map_err(|e| anyhow!("Invalid --ws-multi-ip-base: {}", e))?;
        let base_ip_num: u32 = base_ip.into();
        let ips: Vec<Ipv4Addr> = (0..cli.ws_multi_ip_count)
            .map(|i| Ipv4Addr::from(base_ip_num + i))
            .collect();
        if ips.is_empty() {
            return Err(anyhow!("Multi-IP mode enabled, but IP count is zero."));
        }
        println!("Using {} client IP addresses from {} to {}", ips.len(), ips.first().unwrap(), ips.last().unwrap());
        Arc::new(ips)
    } else {
        Arc::new(vec![]) // Empty vec indicates standard single-IP mode
    };
    // --- End Multi-IP Logic ---

    println!("\nBenchmarking {} WebSocket Chat ({} clients, {} total ops)...", db_name, cli.concurrency, cli.num_ops);
    println!("Connecting clients...");

    let pb = ProgressBar::new(cli.num_ops as u64);
    pb.set_style(ProgressStyle::default_bar().template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}").unwrap().progress_chars("#>-"));
    pb.set_message(format!("{} WS Chat", db_name));
    let pb_arc = Arc::new(pb);

    let total_published_ops = Arc::new(AtomicUsize::new(0));
    let total_received_ops = Arc::new(AtomicUsize::new(0));
    let total_errors = Arc::new(AtomicUsize::new(0));
    let total_bytes_written = Arc::new(AtomicU64::new(0));
    let total_bytes_read = Arc::new(AtomicU64::new(0));

    let mut client_tasks = Vec::with_capacity(cli.concurrency);
    let ops_per_client = (cli.num_ops + cli.concurrency - 1) / cli.concurrency;

    let request = ws_url.into_client_request()?;
    let server_host = request.uri().host().unwrap_or("localhost").to_string();
    let server_port = request.uri().port_u16().unwrap_or(80);
    let server_addr_str = format!("{}:{}", server_host, server_port);
    let server_addr: SocketAddr = server_addr_str.parse()?;

    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
    let all_publishers_done = Arc::new(tokio::sync::Barrier::new(cli.concurrency + 1));
    let all_clients_ready = Arc::new(tokio::sync::Barrier::new(cli.concurrency));


    for worker_id in 0..cli.concurrency {
        let progress_bar_clone = pb_arc.clone();
        let total_published_ops_clone = total_published_ops.clone();
        let total_received_ops_clone = total_received_ops.clone();
        let total_errors_clone = total_errors.clone();
        let total_bytes_written_clone = total_bytes_written.clone();
        let total_bytes_read_clone = total_bytes_read.clone();
        let track_latency_clone = track_latency;
        let ip_addresses_clone = ip_addresses.clone();
        let request_clone = request.clone();
        let cli_clone = cli.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        let all_publishers_done_clone = all_publishers_done.clone();
        let all_clients_ready_clone = all_clients_ready.clone();
        
        client_tasks.push(tokio::spawn(async move {
            let mut hist = if track_latency_clone { Some(hdrhistogram::Histogram::<u64>::new(3).unwrap()) } else { None };
            let mut local_published_ops = 0;
            let mut local_errors = 0;
            let mut rng = StdRng::from_entropy();

            let ws_stream_result = async {
                let socket = TcpSocket::new_v4()?;
                if !ip_addresses_clone.is_empty() {
                    let client_ip = ip_addresses_clone[worker_id % ip_addresses_clone.len()];
                    let bind_addr = SocketAddr::new(std::net::IpAddr::V4(client_ip), 0);
                    socket.bind(bind_addr)?;
                }
                let tcp_stream = socket.connect(server_addr).await?;
                tcp_stream.set_nodelay(true)?;
                let stream = MaybeTlsStream::Plain(tcp_stream);
                client_async(request_clone.clone(), stream).await
            }.await;

            let (mut write, mut read) = match ws_stream_result {
                Ok((ws_stream, _)) => ws_stream.split(),
                Err(e) => {
                    eprintln!("Client worker {}: Failed to establish WS connection: {}", worker_id, e);
                    total_errors_clone.fetch_add(ops_per_client, Ordering::Relaxed);
                    progress_bar_clone.inc(ops_per_client as u64);
                    // Still wait on the barrier to not hang other clients if one fails.
                    all_clients_ready_clone.wait().await;
                    return WorkerResult {
                        histogram: None, errors: ops_per_client, ops_done: 0, bytes_written: 0, bytes_read: 0, active_ws_connections: None, lagged_messages: 0
                    };
                }
            };
            
            // Wait for all other clients to connect before starting work.
            all_clients_ready_clone.wait().await;

            // --- Listener Task ---
            let listener_task = tokio::spawn(async move {
                let mut local_received_ops = 0;
                let mut local_bytes_read = 0;
                let mut listener_errors = 0;

                loop {
                    tokio::select! {
                        biased;
                        _ = shutdown_rx.recv() => break,
                        ws_msg = read.next() => {
                            match ws_msg {
                                Some(Ok(Message::Binary(bin_data))) => {
                                    if process_read_result_simple(&bin_data, cli_clone.format, cli_clone.compress_zstd) {
                                        local_received_ops += 1;
                                        local_bytes_read += bin_data.len() as u64;
                                    } else {
                                        eprintln!("Client worker {}: Failed to process received binary message.", worker_id);
                                        listener_errors += 1;
                                    }
                                },
                                Some(Ok(Message::Close(_))) => break,
                                Some(Ok(_)) => {},
                                Some(Err(_)) => { listener_errors += 1; break; },
                                None => break,
                            }
                        }
                    }
                }
                (local_received_ops, local_bytes_read, listener_errors)
            });

            // --- Publisher Loop ---
            for msg_id_counter in 0..ops_per_client {
                let random_data = generate_random_string(cli_clone.value_size, &mut rng);
                let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                let payload_struct = BenchmarkPayload { data: random_data, timestamp, id: msg_id_counter as usize };
                
                let serialized_bytes = to_bytes::<RkyvError>(&payload_struct).unwrap();
                let msg_len = serialized_bytes.len();
                let message = Message::Binary(serialized_bytes.into_vec().into());
                
                let op_start_time = if track_latency_clone { Some(Instant::now()) } else { None };
                if write.send(message).await.is_ok() {
                    local_published_ops += 1;
                    total_bytes_written_clone.fetch_add(msg_len as u64, Ordering::Relaxed);
                    if let (Some(st), Some(h)) = (op_start_time, hist.as_mut()) { h.record(st.elapsed().as_micros() as u64).unwrap(); }
                } else {
                    local_errors += 1;
                    eprintln!("Client worker {}: Failed to send message.", worker_id);
                    break;
                }
                progress_bar_clone.inc(1);
            }
            
            // Signal that this publisher has finished its work
            all_publishers_done_clone.wait().await;
            
            // Wait for the listener to finish after the global shutdown is triggered
            let (received_ops, bytes_read, listener_errors) = listener_task.await.unwrap_or_default();
            
            total_published_ops_clone.fetch_add(local_published_ops, Ordering::Relaxed);
            total_received_ops_clone.fetch_add(received_ops, Ordering::Relaxed);
            total_bytes_read_clone.fetch_add(bytes_read, Ordering::Relaxed);
            total_errors_clone.fetch_add(local_errors + listener_errors, Ordering::Relaxed);

            WorkerResult {
                histogram: hist,
                errors: local_errors + listener_errors,
                ops_done: local_published_ops,
                bytes_written: 0, // Handled by atomic
                bytes_read: 0, // Handled by atomic
                active_ws_connections: None,
                lagged_messages: 0,
            }
        }));
    }

    println!("All clients connected. Starting benchmark.");
    let start_time = Instant::now();
    
    // Wait for all publisher loops to finish
    all_publishers_done.wait().await;
    
    let total_time = start_time.elapsed();

    // Add a delay to allow listeners to process in-flight messages before shutdown.
    println!("Publishing complete. Waiting for message propagation... (5s)");
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    // Signal all listener tasks to shut down
    let _ = shutdown_tx.send(());
    
    // Wait for all client tasks (which includes their listeners) to complete
    let worker_results = futures::future::join_all(client_tasks).await;

    pb_arc.finish_with_message("Done");
    
    let mut final_histogram = hdrhistogram::Histogram::<u64>::new(3).unwrap();
    for result in worker_results {
        if let Ok(wr) = result {
            if let Some(h) = wr.histogram {
                final_histogram.add(&h).unwrap();
            }
        }
    }

    let final_published_ops = total_published_ops.load(Ordering::SeqCst);
    let final_received_ops = total_received_ops.load(Ordering::SeqCst);
    let final_errors = total_errors.load(Ordering::SeqCst);
    let final_bytes_written = total_bytes_written.load(Ordering::SeqCst);
    let final_bytes_read = total_bytes_read.load(Ordering::SeqCst);

    let ops_per_second_published = if total_time.as_secs_f64() > 0.0 { final_published_ops as f64 / total_time.as_secs_f64() } else { 0.0 };
    let ops_per_second_received = if total_time.as_secs_f64() > 0.0 { final_received_ops as f64 / total_time.as_secs_f64() } else { 0.0 };

    let (avg_lat, p99_lat) = if track_latency && final_histogram.len() > 0 {
        (Some(final_histogram.mean() / 1000.0), Some(final_histogram.value_at_percentile(99.0) as f64 / 1000.0))
    } else {
        (None, None)
    };
    
    println!("\n--- WebSocket Chat Benchmark Results ---");
    println!("Total Published Messages: {}", final_published_ops);
    println!("Total Received Messages: {}", final_received_ops);
    println!("Published Ops/sec: {:.2}", ops_per_second_published);
    println!("Received Ops/sec: {:.2}", ops_per_second_received);
    println!("Total Time: {:?}", total_time);
    println!("Total Errors: {}", final_errors);
    println!("Total Bytes Written: {}", final_bytes_written);
    println!("Total Bytes Read: {}", final_bytes_read);


    Ok(BenchResult {
        ops_per_second: ops_per_second_published,
        total_time,
        avg_latency_ms: avg_lat,
        p99_latency_ms: p99_lat,
        errors: final_errors,
        total_ops_completed: final_published_ops,
        total_bytes_written: final_bytes_written,
        total_bytes_read: final_bytes_read,
        total_lagged_messages: 0,
    })
}