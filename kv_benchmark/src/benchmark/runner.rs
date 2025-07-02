use crate::benchmark::data::{BenchResult, BenchmarkPayload, PreGeneratedData, WorkerResult};
use crate::cli::{Cli, DataFormat, Workload};
use crate::db::KvStore;
use crate::{fbs, pb};
use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};
use prost::Message;
use rand::{prelude::*, rngs::StdRng};
use rkyv::{access, rancor::Error as RkyvError, Archived};
use std::sync::Arc;
use std::time::Instant;
use zstd::stream::decode_all;

pub async fn run_benchmark(db_name: &str, op_type_ref: &str, db: Box<dyn KvStore + Send + Sync>, cli: &Cli, data: &PreGeneratedData, track_latency: bool) -> Result<BenchResult> {
    let op_type_owned = op_type_ref.to_string();
    let zero_copy_description = if op_type_owned == "READ" && matches!(cli.format, DataFormat::Rkyv | DataFormat::Flatbuffers) { " (Zero-Copy Deserialization)" } else { "" };
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
                                Ok(results) => { local_bytes_read += results.iter().map(|b| b.len() as u64).sum::<u64>(); for bytes in results { if !process_read_result_simple(&bytes, cli_clone.format, cli_clone.compress_zstd) { local_errors += 1; } } Ok(()) },
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
                            else { match db_clone.get(key).await { Ok(Some(bytes)) => { local_bytes_read += bytes.len() as u64; process_read_result_simple(&bytes, cli_clone.format, cli_clone.compress_zstd) }, Ok(None) => true, Err(_) => false, } }
                        }
                    };
                    if let Some(st) = op_start_time { if let Some(h) = hist.as_mut() { h.record(st.elapsed().as_micros() as u64).unwrap(); } }
                    if !op_successful { local_errors += 1; }
                    local_ops_done += 1;
                    progress_bar_clone.inc(1);
                }
            }
            WorkerResult { histogram: hist, errors: local_errors, ops_done: local_ops_done, bytes_written: local_bytes_written, bytes_read: local_bytes_read, active_ws_connections: None }
        }));
    }
    let worker_results = futures::future::join_all(tasks).await;
    let total_time = start_time.elapsed();
    pb_arc.finish_with_message("Done");
    let mut final_histogram = if track_latency && !cli.pipeline { Some(hdrhistogram::Histogram::<u64>::new(3).unwrap()) } else { None };
    let mut total_errors = 0;
    let mut total_ops_completed = 0;
    let mut total_bytes_written = 0;
    let mut total_bytes_read = 0;
    for result in worker_results {
        if let Ok(wr) = result {
            if let (Some(fh), Some(wh)) = (final_histogram.as_mut(), wr.histogram.as_ref()) { fh.add(wh).unwrap(); }
            total_errors += wr.errors;
            total_ops_completed += wr.ops_done;
            total_bytes_written += wr.bytes_written;
            total_bytes_read += wr.bytes_read;
        } else {
            eprintln!("Worker task panicked.");
            total_errors += ops_per_worker;
            total_ops_completed += ops_per_worker;
        }
    }
    if total_errors > 0 { println!("WARNING: {}/{} operations reported errors.", total_errors, cli.num_ops); }
    let successful_ops = total_ops_completed.saturating_sub(total_errors);
    let ops_per_second = if total_time.as_secs_f64() > 0.0 { successful_ops as f64 / total_time.as_secs_f64() } else { 0.0 };
    let (avg_lat, p99_lat) = if let Some(hist) = final_histogram.as_ref() { (Some(hist.mean() / 1000.0), Some(hist.value_at_percentile(99.0) as f64 / 1000.0)) } else { (None, None) };
    Ok(BenchResult { ops_per_second, total_time, avg_latency_ms: avg_lat, p99_latency_ms: p99_lat, errors: total_errors, total_ops_completed, total_bytes_written, total_bytes_read })
}

pub fn process_read_result_chat(messages: Vec<Vec<u8>>, format: DataFormat, compressed: bool) -> bool {
    messages.into_iter().all(|bytes_vec| process_read_result_simple(bytes_vec.as_slice(), format, compressed))
}

pub fn process_read_result_simple(bytes_slice: &[u8], format: DataFormat, compressed: bool) -> bool {
    if bytes_slice.is_empty() { return true; }

    let owned_decompressed_opt: Option<Vec<u8>> = if compressed {
        match decode_all(bytes_slice) {
            Ok(decompressed) => Some(decompressed),
            Err(_) => return false,
        }
    } else {
        None
    };

    let final_bytes_slice: &[u8] = if let Some(ref owned_vec) = owned_decompressed_opt {
        owned_vec.as_slice()
    } else {
        bytes_slice
    };

    match format {
        DataFormat::Rkyv => access::<Archived<BenchmarkPayload>, RkyvError>(final_bytes_slice).map(|_| true).unwrap_or(false),
        DataFormat::Flatbuffers => flatbuffers::root::<fbs::benchmark_fbs::BenchmarkPayload>(final_bytes_slice).is_ok(),
        DataFormat::Protobuf => pb::BenchmarkPayload::decode(final_bytes_slice).is_ok(),
        DataFormat::Json => serde_json::from_slice::<BenchmarkPayload>(final_bytes_slice).is_ok(),
        DataFormat::Bitcode => bitcode::deserialize::<BenchmarkPayload>(final_bytes_slice).is_ok(),
        DataFormat::String => String::from_utf8(final_bytes_slice.to_vec()).is_ok(),
    }
}