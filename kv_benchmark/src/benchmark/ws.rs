use crate::benchmark::data::{BenchResult, PreGeneratedData, WorkerResult};
use crate::cli::Cli;
use anyhow::{anyhow, Result};
use futures::{stream::StreamExt, SinkExt};
use indicatif::{ProgressBar, ProgressStyle};
use std::net::{Ipv4Addr, SocketAddr};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::net::{TcpSocket, TcpStream};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{client_async, MaybeTlsStream, WebSocketStream};

pub async fn run_ws_benchmark(db_name: &str, ws_url: &str, cli: &Cli, _data: &PreGeneratedData, track_latency: bool) -> Result<BenchResult> {
    if cli.num_ops == 0 {
        return Err(anyhow!("Target number of connections (num-ops) must be greater than 0."));
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

    println!("\nBenchmarking {} WebSocket Connection Capacity (Target: {} connections, {} concurrent clients)...", db_name, cli.num_ops, cli.concurrency);

    let pb = ProgressBar::new(cli.num_ops as u64);
    pb.set_style(ProgressStyle::default_bar().template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}").unwrap().progress_chars("#>-"));
    pb.set_message(format!("{} WS Connections", db_name));
    let pb_arc = Arc::new(pb);

    let established_connections_total = Arc::new(AtomicUsize::new(0));
    let failed_connections_total = Arc::new(AtomicUsize::new(0));

    let mut client_tasks = Vec::with_capacity(cli.concurrency);
    let connections_per_worker = (cli.num_ops + cli.concurrency - 1) / cli.concurrency;

    let total_start_time = Instant::now();
    let request = ws_url.into_client_request()?;
    let server_host = request.uri().host().unwrap_or("localhost").to_string();
    let server_port = request.uri().port_u16().unwrap_or(80);
    let server_addr_str = format!("{}:{}", server_host, server_port);
    let server_addr: SocketAddr = server_addr_str.parse()?;

    for worker_id in 0..cli.concurrency {
        let progress_bar_clone = pb_arc.clone();
        let established_connections_clone = established_connections_total.clone();
        let failed_connections_clone = failed_connections_total.clone();
        let track_latency_clone = track_latency;
        let ip_addresses_clone = ip_addresses.clone();
        let request_clone = request.clone();

        client_tasks.push(tokio::spawn(async move {
            let mut hist = if track_latency_clone { Some(hdrhistogram::Histogram::<u64>::new(3).unwrap()) } else { None };
            let mut local_established_count = 0;
            let mut local_failed_count = 0;
            let mut active_connections: Vec<WebSocketStream<MaybeTlsStream<TcpStream>>> = Vec::with_capacity(connections_per_worker);

            for i in 0..connections_per_worker {
                let op_start_time = if track_latency_clone { Some(Instant::now()) } else { None };

                let connection_result = async {
                    let socket = TcpSocket::new_v4()?;

                    if !ip_addresses_clone.is_empty() {
                        let client_ip = ip_addresses_clone[(worker_id * connections_per_worker + i) % ip_addresses_clone.len()];
                        let bind_addr = SocketAddr::new(std::net::IpAddr::V4(client_ip), 0);
                        socket.bind(bind_addr)?;
                    }

                    let tcp_stream = socket.connect(server_addr).await?;
                    tcp_stream.set_nodelay(true)?;

                    // **THE FIX IS HERE**: Wrap the plain TCP stream in MaybeTlsStream::Plain
                    let stream = MaybeTlsStream::Plain(tcp_stream);

                    // Upgrade the wrapped stream to a WebSocket connection.
                    // The result is now WebSocketStream<MaybeTlsStream<TcpStream>>, which matches our Vec type.
                    let (ws_stream, _) = client_async(request_clone.clone(), stream).await?;

                    Ok::<_, anyhow::Error>(ws_stream)
                }.await;

                match connection_result {
                    Ok(ws_stream) => {
                        local_established_count += 1;
                        established_connections_clone.fetch_add(1, Ordering::Relaxed);
                        active_connections.push(ws_stream);
                        if let (Some(st), Some(h)) = (op_start_time, hist.as_mut()) { h.record(st.elapsed().as_micros() as u64).unwrap(); }
                    },
                    Err(e) => {
                        local_failed_count += 1;
                        failed_connections_clone.fetch_add(1, Ordering::Relaxed);
                         if local_failed_count <= 5 {
                             eprintln!("Worker {}: Failed to connect (attempt {}): {}", worker_id, i, e);
                         } else if local_failed_count == 6 {
                             eprintln!("Worker {}: ... (further connection errors suppressed for this worker)", worker_id);
                         }
                    }
                }
                progress_bar_clone.inc(1);
            }

            WorkerResult {
                histogram: hist,
                errors: local_failed_count,
                ops_done: local_established_count,
                bytes_written: 0,
                bytes_read: 0,
                active_ws_connections: Some(active_connections),
            }
        }));
    }

    let worker_results = futures::future::join_all(client_tasks).await;
    let total_time_taken_to_establish = total_start_time.elapsed();
    pb_arc.finish_with_message("Done");

    let mut final_histogram = if track_latency { Some(hdrhistogram::Histogram::<u64>::new(3).unwrap()) } else { None };
    let total_connections_established = established_connections_total.load(Ordering::SeqCst);
    let total_connections_failed = failed_connections_total.load(Ordering::SeqCst);
    let mut all_active_connections = Vec::with_capacity(total_connections_established);
    for result in worker_results {
        if let Ok(mut wr) = result {
            if let (Some(fh), Some(wh)) = (final_histogram.as_mut(), wr.histogram.as_ref()) { fh.add(wh).unwrap(); }
            if let Some(conns) = wr.active_ws_connections.take() { all_active_connections.extend(conns); }
        } else {
            eprintln!("A worker task panicked. This may affect total counts reported.");
        }
    }
    let connections_per_second = if total_time_taken_to_establish.as_secs_f64() > 0.0 { total_connections_established as f64 / total_time_taken_to_establish.as_secs_f64() } else { 0.0 };
    let (avg_lat, p99_lat) = if let Some(hist) = final_histogram.as_ref() { (Some(hist.mean() / 1000.0), Some(hist.value_at_percentile(99.0) as f64 / 1000.0)) } else { (None, None) };

    println!("\n--- Connection Capacity Benchmark Results ---");
    println!("Target connections: {}", cli.num_ops);
    println!("Successfully established: {}", total_connections_established);
    println!("Failed to establish: {}", total_connections_failed);
    println!("Total time to establish: {:?}", total_time_taken_to_establish);
    println!("Establishment Rate: {:.2} connections/sec", connections_per_second);

    if !all_active_connections.is_empty() {
        let hold_duration = std::time::Duration::from_secs(30);
        println!("\nHolding {} connections open for {:?} to measure stability...", all_active_connections.len(), hold_duration);
        tokio::time::sleep(hold_duration).await;
        println!("Hold period finished. Closing connections.");
    }

    std::mem::drop(all_active_connections);

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