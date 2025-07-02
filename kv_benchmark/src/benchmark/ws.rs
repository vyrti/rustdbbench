use crate::benchmark::data::{BenchResult, BenchmarkPayload, PreGeneratedData, WorkerResult};
use crate::benchmark::data::generate_random_string; // Corrected: Use specific import
use crate::benchmark::runner::process_read_result_simple;
use crate::cli::Cli; // Removed DataFormat as it's not directly used here
use anyhow::{anyhow, Result};
use futures::{stream::StreamExt, SinkExt};
use indicatif::{ProgressBar, ProgressStyle};
use std::net::{Ipv4Addr, SocketAddr};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering}; // Corrected: Added AtomicU64
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::net::{TcpSocket, TcpStream}; // Removed TcpStream as it's re-exported by MaybeTlsStream
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{client_async, MaybeTlsStream, WebSocketStream}; // Removed WebSocketStream as it's re-exported
use rand::{prelude::*, rngs::StdRng};
use rkyv::{rancor::Error as RkyvError, to_bytes, access, Archived}; // Corrected: Use access, Archived

pub async fn run_ws_benchmark(db_name: &str, ws_url: &str, cli: &Cli, data: &PreGeneratedData, track_latency: bool) -> Result<BenchResult> {
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

    let pb = ProgressBar::new(cli.num_ops as u64);
    pb.set_style(ProgressStyle::default_bar().template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}").unwrap().progress_chars("#>-"));
    pb.set_message(format!("{} WS Chat", db_name));
    let pb_arc = Arc::new(pb);

    let total_published_ops = Arc::new(AtomicUsize::new(0));
    let total_received_ops = Arc::new(AtomicUsize::new(0));
    let total_errors = Arc::new(AtomicUsize::new(0)); // For connection errors
    let total_bytes_written = Arc::new(AtomicU64::new(0));
    let total_bytes_read = Arc::new(AtomicU64::new(0));

    let mut client_tasks = Vec::with_capacity(cli.concurrency);
    let ops_per_client = cli.num_ops / cli.concurrency; // Ops per client for publishing

    let request = ws_url.into_client_request()?;
    let server_host = request.uri().host().unwrap_or("localhost").to_string();
    let server_port = request.uri().port_u16().unwrap_or(80);
    let server_addr_str = format!("{}:{}", server_host, server_port);
    let server_addr: SocketAddr = server_addr_str.parse()?;

    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);


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
        let mut shutdown_rx = shutdown_tx.subscribe(); // Corrected: make shutdown_rx mutable
        let keys_ref = Arc::clone(&data.keys); // All clients can pick from these keys (chat rooms)


        client_tasks.push(tokio::spawn(async move {
            let mut hist = if track_latency_clone { Some(hdrhistogram::Histogram::<u64>::new(3).unwrap()) } else { None };
            let mut local_published_ops = 0;
            let mut local_received_ops = 0;
            let mut local_errors = 0;
            let mut local_bytes_written = 0;
            let mut local_bytes_read = 0;
            let mut rng = StdRng::from_entropy();

            let ws_stream_result = async {
                let socket = TcpSocket::new_v4()?;
                if !ip_addresses_clone.is_empty() {
                    let client_ip = ip_addresses_clone[(worker_id * ops_per_client) % ip_addresses_clone.len()];
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
                    total_errors_clone.fetch_add(1, Ordering::Relaxed);
                    return WorkerResult {
                        histogram: hist, errors: 1, ops_done: 0, bytes_written: 0, bytes_read: 0, active_ws_connections: None
                    };
                }
            };

            // Main loop for chat operations (publish and receive)
            let mut msg_id_counter = 0;
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown_rx.recv() => { // Corrected: shutdown_rx is mutable now
                        let _ = write.close().await;
                        break;
                    },
                    // Simulate publishing a message
                    _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                        if local_published_ops >= ops_per_client && ops_per_client > 0 {
                            continue;
                        }

                        let _chat_room_key = &keys_ref[rng.gen_range(0..keys_ref.len())]; // Corrected: prefix with _
                        let random_data = generate_random_string(cli_clone.value_size, &mut rng);
                        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                        let payload_struct = BenchmarkPayload { data: random_data, timestamp, id: msg_id_counter as usize };
                        msg_id_counter += 1;

                        // Serialize with Rkyv
                        let serialized_bytes = to_bytes::<RkyvError>(&payload_struct).unwrap().into_vec();
                        let message = Message::Binary(serialized_bytes.into());

                        let op_start_time = if track_latency_clone { Some(Instant::now()) } else { None };
                        if write.send(message).await.is_ok() {
                            local_published_ops += 1;
                            local_bytes_written += payload_struct.data.len() as u64;
                            progress_bar_clone.inc(1);
                            if let (Some(st), Some(h)) = (op_start_time, hist.as_mut()) { h.record(st.elapsed().as_micros() as u64).unwrap(); }
                        } else {
                            local_errors += 1;
                            eprintln!("Client worker {}: Failed to send message.", worker_id);
                            break;
                        }
                    }
                    // Listen for incoming messages (fan-out from other clients)
                    ws_msg = read.next() => {
                        match ws_msg {
                            Some(Ok(Message::Binary(bin_data))) => {
                                if process_read_result_simple(&bin_data, cli_clone.format, cli_clone.compress_zstd) {
                                    local_received_ops += 1;
                                    local_bytes_read += bin_data.len() as u64;
                                } else {
                                    eprintln!("Client worker {}: Failed to process received binary message.", worker_id);
                                    local_errors += 1;
                                }
                            },
                            Some(Ok(Message::Close(_))) => { break; },
                            Some(Ok(_)) => {},
                            Some(Err(e)) => {
                                eprintln!("Client worker {}: Error receiving message: {}", worker_id, e);
                                local_errors += 1;
                                break;
                            },
                            None => { break; },
                        }
                    }
                    else => if ops_per_client > 0 && local_published_ops >= ops_per_client {
                        let _ = write.close().await;
                        break;
                    }
                }
            }
            
            total_published_ops_clone.fetch_add(local_published_ops, Ordering::Relaxed);
            total_received_ops_clone.fetch_add(local_received_ops, Ordering::Relaxed);
            total_errors_clone.fetch_add(local_errors, Ordering::Relaxed);
            total_bytes_written_clone.fetch_add(local_bytes_written, Ordering::Relaxed);
            total_bytes_read_clone.fetch_add(local_bytes_read, Ordering::Relaxed);

            WorkerResult {
                histogram: hist, errors: local_errors, ops_done: local_published_ops,
                bytes_written: local_bytes_written, bytes_read: local_bytes_read, active_ws_connections: None
            }
        }));
    }

    let start_time = Instant::now();
    futures::future::join_all(client_tasks).await;
    let total_time = start_time.elapsed();
    let _ = shutdown_tx.send(());
    pb_arc.finish_with_message("Done");

    let final_published_ops = total_published_ops.load(Ordering::SeqCst);
    let final_received_ops = total_received_ops.load(Ordering::SeqCst);
    let final_errors = total_errors.load(Ordering::SeqCst);
    let final_bytes_written = total_bytes_written.load(Ordering::SeqCst);
    let final_bytes_read = total_bytes_read.load(Ordering::SeqCst);

    let ops_per_second_published = if total_time.as_secs_f64() > 0.0 { final_published_ops as f64 / total_time.as_secs_f64() } else { 0.0 };
    let ops_per_second_received = if total_time.as_secs_f64() > 0.0 { final_received_ops as f64 / total_time.as_secs_f64() } else { 0.0 };

    let _final_histogram: Option<hdrhistogram::Histogram<u64>> = None; // Corrected: prefix with _ and remove mut
    let (avg_lat, p99_lat) = if track_latency { (Some(0.0), Some(0.0)) } else { (None, None) };


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
    })
}