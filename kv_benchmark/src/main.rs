use anyhow::Result;
use clap::Parser;
use std::sync::Arc;
use byte_unit::{Byte, UnitType};

mod cli;
mod db;
mod benchmark;
mod ws_server;

// These must be in the crate root for `include!` to work with the build script's OUT_DIR
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/benchmark.rs"));
}
#[allow(dead_code, unused_imports)]
pub mod fbs {
    include!(concat!(env!("OUT_DIR"), "/benchmark_generated.rs"));
}

use cli::{Cli, DbChoice, Workload};
use db::{in_memory::InMemoryStore, redis::RedisStore, KvStore};
use benchmark::{
    data::PreGeneratedData,
    pubsub::{run_internal_pubsub_benchmark, run_pubsub_benchmark, InternalPubSub}, // Import InternalPubSub
    runner::run_benchmark,
    ws::run_ws_benchmark,
};
use ws_server::run_axum_ws_server;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Check for incompatible flags
    if cli.db == DbChoice::WebSocket && (cli.write_only || cli.read_only || cli.pubsub_only) {
        eprintln!("Error: When --db WebSocket is chosen, other benchmark flags like --write-only, --read-only, or --pubsub-only are not applicable as it runs a dedicated connection capacity test.");
        return Ok(());
    }
    if cli.db == DbChoice::WebSocketChat && (cli.write_only || cli.read_only || cli.pubsub_only) {
        eprintln!("Error: When --db WebSocketChat is chosen, other benchmark flags like --write-only, --read-only, or --pubsub-only are not applicable as it runs a dedicated chat emulation benchmark.");
        return Ok(());
    }

    if cli.run_ws_server {
        println!("\n--- Starting Axum WebSocket Server ---");
        // For the WebSocketChat scenario, the server needs an InternalPubSub instance
        let internal_pubsub = if cli.db == DbChoice::WebSocketChat {
            Arc::new(InternalPubSub::new(1024)) // Larger channel size for chat fan-out
        } else {
            Arc::new(InternalPubSub::new(256)) // Default for non-chat if needed later
        };
        run_axum_ws_server(internal_pubsub, cli.format, cli.compress_zstd).await?;
    } else {
        println!("Configuration: DB={:?}, Workload={:?}, Ops={}, Concurrency={}, Format={:?}, ValueBaseSize={}B", cli.db, cli.workload, cli.num_ops, cli.concurrency, cli.format, cli.value_size);
        if cli.pubsub_only && cli.db != DbChoice::WebSocket { println!("Pub/Sub mode: Publishers={}", cli.num_publishers); }
        if cli.workload == Workload::Chat { println!("Chat Sim: Chats={}, HistoryLen={}, ReadSize={}", cli.num_chats, cli.history_len, cli.read_size); }
        println!("Technical: Pipeline={}, BatchSize={}, NoLatency={}, Compression={}", cli.pipeline, cli.batch_size, cli.no_latency, if cli.compress_zstd {"zstd"} else {"none"});

        if cli.db != DbChoice::InMemory {
            println!("Ensure Redis ({}), Valkey ({}), RustDb ({}) and WebSocket Server ({}) are running", cli.redis_url, cli.valkey_url, cli.rustdb_url, cli.ws_url);
        }

        if cli.num_ops == 0 { println!("Number of operations is 0, exiting."); return Ok(()); }

        let benchmarks_to_run = match cli.db {
            DbChoice::WebSocket => vec!["WS_CONNECTIONS"],
            DbChoice::WebSocketChat => vec!["WS_CHAT_EMULATION"], // New benchmark type
            _ => { // Existing logic for other DB types
                match (cli.write_only, cli.read_only, cli.pubsub_only) {
                    (true, false, false) => vec!["WRITE"],
                    (false, true, false) => vec!["READ"],
                    (false, false, true) => vec!["PUBSUB"],
                    (false, false, false) => vec!["WRITE", "READ"],
                    _ => { eprintln!("Error: Please specify at most one of --write-only, --read-only, or --pubsub-only for non-WebSocket benchmarks."); return Ok(()); }
                }
            }
        };


        // Data generation: For chat, we need keys (chat rooms) and values (messages).
        // For WS_CONNECTIONS, `_data` is unused.
        let op_for_data_gen = if benchmarks_to_run.contains(&"WS_CHAT_EMULATION") {
            "WRITE" // Need messages to send, so treat as a write operation
        } else if benchmarks_to_run.contains(&"PUBSUB") {
            match cli.db {
                DbChoice::InMemory | DbChoice::RustDb => "PUBSUB_INTERNAL",
                _ => "PUBSUB",
            }
        } else {
            "WRITE"
        };
        let data = PreGeneratedData::new(&cli, op_for_data_gen);


        let db_choices = vec![cli.db];

        for db_choice in db_choices {
            let db_name = format!("{:?}", db_choice);

            let store_option: Option<Box<dyn KvStore + Send + Sync>> = if db_choice != DbChoice::WebSocket && db_choice != DbChoice::WebSocketChat {
                Some(match db_choice {
                    DbChoice::Redis => Box::new(RedisStore::new(&cli.redis_url).await?),
                    DbChoice::Valkey => Box::new(RedisStore::new(&cli.valkey_url).await?),
                    DbChoice::InMemory => Box::new(InMemoryStore::new()),
                    DbChoice::RustDb => Box::new(RedisStore::new(&cli.rustdb_url).await?),
                    _ => unreachable!(), // Handled above
                })
            } else {
                None
            };

            if benchmarks_to_run.contains(&"READ") && store_option.is_some() {
                let mut prepop_cli = cli.clone();
                prepop_cli.pipeline = true;
                prepop_cli.batch_size = cli.batch_size.max(200);
                println!("\nINFO: Pre-populating {} for READ benchmark...", db_name);
                run_benchmark(&format!("{}-PrePop", db_name), "WRITE", store_option.as_ref().unwrap().clone(), &prepop_cli, &data, false).await?;
            }

            let mut results_table = Vec::new();
            for op_type_str_ref in &benchmarks_to_run {
                let actual_track_latency = !cli.no_latency && !cli.pipeline;

                let result = match *op_type_str_ref {
                    "WS_CONNECTIONS" => run_ws_benchmark(&db_name, &cli.ws_url, &cli, &data, actual_track_latency).await,
                    "WS_CHAT_EMULATION" => run_ws_benchmark(&db_name, &cli.ws_url, &cli, &data, actual_track_latency).await, // Reuse run_ws_benchmark for now, it handles the logic
                    "PUBSUB" => {
                        match db_choice {
                            DbChoice::Redis => run_pubsub_benchmark(&db_name, &cli.redis_url, &cli, &data, !cli.no_latency).await,
                            DbChoice::Valkey => run_pubsub_benchmark(&db_name, &cli.valkey_url, &cli, &data, !cli.no_latency).await,
                            DbChoice::InMemory | DbChoice::RustDb => run_internal_pubsub_benchmark(&db_name, &cli, &data, !cli.no_latency).await,
                            _ => unreachable!(),
                        }
                    },
                    _ => { // WRITE, READ
                        let store_ref = store_option.as_ref().expect("KvStore should be initialized for non-PubSub/WS benchmarks");
                        let current_data = if *op_type_str_ref == "WRITE" { &data } else { &PreGeneratedData { keys: Arc::clone(&data.keys), values: None } };
                        run_benchmark(&db_name, op_type_str_ref, store_ref.clone(), &cli, current_data, actual_track_latency).await
                    },
                };

                match result {
                    Ok(res) => { results_table.push(( db_name.clone(), *op_type_str_ref, res.ops_per_second, res.total_time.as_secs_f64(), res.avg_latency_ms, res.p99_latency_ms, res.errors, res.total_ops_completed, res.total_bytes_written, res.total_bytes_read )); },
                    Err(e) => { eprintln!("Error benchmarking {} {}: {}", db_name, op_type_str_ref, e); }
                }
            }

            print_summary_table(&db_name, &cli, &results_table, db_choice);
        }
    }

    Ok(())
}

type ResultsData<'a> = &'a [(String, &'a str, f64, f64, Option<f64>, Option<f64>, usize, usize, u64, u64)];

fn print_summary_table(db_name: &str, cli: &Cli, results_table: ResultsData, db_choice: DbChoice) {
    let summary_workload = match db_choice {
        DbChoice::WebSocket => "Connection Capacity".to_string(),
        DbChoice::WebSocketChat => "Chat Emulation".to_string(),
        _ if cli.pubsub_only => "Pub/Sub".to_string(),
        _ => format!("{:?}", cli.workload),
    };
    println!("\n--- Benchmark Summary (DB: {}, Format: {:?}{}, Workload: {}) ---", db_name, cli.format, if cli.compress_zstd { "+zstd" } else { "" }, summary_workload);
    let show_latency_in_table = !cli.no_latency && !cli.pipeline;
    let op_col_width = 24;
    if show_latency_in_table {
        println!("{:<12} | {:<op_col_width$} | {:<14} | {:<14} | {:<14} | {:<12} | {:<12} | {:<8}", "Database", "Op Type", "Rate/Sec", "Speed (MB/s)", "Total Traffic", "Avg Lat(ms)", "P99 Lat(ms)", "Errors");
        println!("{:-<13}|{:-<op_col_width$}|{:-<16}|{:-<16}|{:-<16}|{:-<14}|{:-<14}|{:-<10}", "-", "-", "-", "-", "-", "-", "-", "-");
    } else {
        println!("{:<12} | {:<op_col_width$} | {:<14} | {:<14} | {:<14} | {:<8}", "Database", "Op Type", "Rate/Sec", "Speed (MB/s)", "Total Traffic", "Errors");
        println!("{:-<13}|{:-<op_col_width$}|{:-<16}|{:-<16}|{:-<16}|{:-<10}", "-", "-", "-", "-", "-", "-");
    }

    for (db, op, ops_sec, time_s, avg_lat, p99_lat, errors, total_ops_completed, bytes_written, bytes_read) in results_table.iter() {
        let op_name = if *op == "READ" {
            let zc_suffix = match cli.format {
                cli::DataFormat::Rkyv | cli::DataFormat::Flatbuffers => " (Zero-Copy)",
                _ => "",
            };
            format!("READ ({:?}){}", cli.workload, zc_suffix)
        } else if *op == "PUBSUB" {
            let suffix = match db_choice {
                DbChoice::InMemory | DbChoice::RustDb => " (Internal)".to_string(),
                _ => "".to_string(),
            };
            format!("PUBSUB ({} Pubs){}", cli.num_publishers, suffix)
        } else if *op == "WS_CONNECTIONS" {
            format!("WS Conn ({} Target)", cli.num_ops)
        } else if *op == "WS_CHAT_EMULATION" {
            format!("WS Chat ({} Clients)", cli.concurrency)
        }
        else {
            format!("WRITE ({:?})", cli.workload)
        };

        let total_traffic_bytes = *bytes_written + *bytes_read;
        let traffic_speed_mb_s = if *time_s > 0.0 { (total_traffic_bytes as f64 / *time_s) / 1_000_000.0 } else { 0.0 };
        let total_traffic_str = Byte::from(total_traffic_bytes).get_appropriate_unit(UnitType::Binary).to_string();

        let primary_rate_val = if *op == "WS_CONNECTIONS" {
            *total_ops_completed as f64 // For connection benchmark, show total established
        } else if *op == "WS_CHAT_EMULATION" {
            // For chat, display the 'published' ops/sec, as it's the active traffic driver
            *ops_sec
        }
        else {
            *ops_sec
        };


        if show_latency_in_table {
            println!("{:<12} | {:<op_col_width$} | {:<14.2} | {:<14.2} | {:<14} | {:<12.3} | {:<12.3} | {:<8}", db, op_name, primary_rate_val, traffic_speed_mb_s, total_traffic_str, avg_lat.unwrap_or(0.0), p99_lat.unwrap_or(0.0), errors);
        } else {
            println!("{:<12} | {:<op_col_width$} | {:<14.2} | {:<14.2} | {:<14} | {:<8}", db, op_name, primary_rate_val, traffic_speed_mb_s, total_traffic_str, errors);
        }
    }
}