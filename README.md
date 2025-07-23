# Rust KV Store & Benchmark Suite

This project contains two main components:
1.  **`rustdb`**: A high-performance, multi-threaded, in-memory key-value store, built with Rust and Tokio, that speaks a subset of the Redis (RESP) protocol.
2.  **`kv_benchmark`**: A comprehensive benchmarking suite to test `rustdb` and compare its performance against other backends like Redis, Valkey, NATS, and WebSockets under various workloads and data formats.

This project was developed with assistance from AI coding tools.

## Features

### `rustdb` (In-Memory Server)

-   **High-Performance**: Built on `tokio` for asynchronous I/O and uses high-performance libraries like `parking_lot` for locks and `rustc_hash` for hashing.
-   **Concurrent**: Multi-threaded architecture to leverage modern multi-core processors.
-   **Sharded Data Store**: The internal hashmap is sharded to significantly reduce lock contention under high concurrency.
-   **Redis Protocol Subset**: Supports `GET`, `SET`, `LPUSH`, `LRANGE`, `LTRIM`, and `PING` commands, allowing interaction with standard Redis clients.

### `kv_benchmark` (Benchmark Tool)

-   **Multi-Backend Testing**: Natively supports benchmarking `rustdb`, Redis, Valkey, NATS, and a custom WebSocket server.
-   **Versatile Workloads**:
    -   `Simple`: Basic Key-Value `GET`/`SET` operations.
    -   `Chat`: Simulates a chat application using Redis Lists (`LPUSH`/`LRANGE`).
    -   `Pub/Sub`: Tests message broadcasting and fan-out performance.
    -   `WebSocket`: Emulates a real-time chat application over WebSockets to test connection scaling and message passing.
-   **Serialization Analysis**: Compare the performance impact of different data formats:
    -   `String` (raw bytes)
    -   `JSON` (`serde_json`)
    -   `Bitcode` (compact binary format)
    -   `Protobuf` (`prost`)
    -   `Rkyv` & `Flatbuffers` (for zero-copy deserialization)
-   **Configurable Parameters**: Fine-tune benchmarks by adjusting concurrency, total operations, data size, pipelining, batch sizes, and data compression (zstd).
-   **Detailed Metrics**: Outputs clear summary tables with throughput (ops/sec), data transfer rates, total time, errors, and optional latency stats (average and p99).

## Getting Started

### Prerequisites

-   [Rust Toolchain](https://www.rust-lang.org/tools/install) (`rustc`, `cargo`)
-   [Docker and Docker Compose](https://docs.docker.com/get-docker/)

### Setup

1.  **Clone the Repository**
    ```sh
    git clone <repository-url>
    cd <repository-directory>
    ```

2.  **Start Backend Services**
    Use Docker Compose to start Redis, Valkey, and NATS. This command also pulls the necessary images.
    ```sh
    docker-compose up -d
    ```
    -   Redis will be available on `127.0.0.1:6379`.
    -   Valkey will be available on `127.0.0.1:6380`.
    -   NATS will be available on `127.0.0.1:4222`.

    > **Note**: The `docker-compose.yml` is configured for `linux/arm64`. If you are on an x86-64 machine, you may need to remove the `platform` line from the `docker-compose.yml` file for each service.

3.  **Build the Project**
    Convenience scripts are provided to build the server and the benchmark tool with performance optimizations.

    ```sh
    # Build the 'rustdb' server
    bash builddb.sh
    
    # Build the 'kv_benchmark' tool
    bash build.sh
    ```
    The optimized executables will be located at:
    -   Server: `db/target/maxperf/rustdb`
    -   Benchmark: `kv_benchmark/target/maxperf/kv_benchmark`

## How to Run

### 1. Start the `rustdb` Server (Optional)

If you want to benchmark the custom `rustdb` server, you must start it first.

```sh
./db/target/maxperf/rustdb
```

By default, it listens on 127.0.0.1:7878. You can change this with the --addr flag.
2. Run the Benchmarks

The benchmark tool is highly configurable via command-line arguments.

Example 1: Basic Benchmark (Simple workload)
Run a Simple GET/SET benchmark against Redis with default settings.

```sh
./kv_benchmark/target/maxperf/kv_benchmark --db redis --workload Simple
```

Example 2: Compare rustdb, Redis, and Valkey (Chat workload)
Run a Chat simulation workload, comparing all three key-value stores.
      
# Run against rustdb
```sh
./kv_benchmark/target/maxperf/kv_benchmark --db RustDb --workload Chat
```

# Run against Redis
```sh
./kv_benchmark/target/maxperf/kv_benchmark --db Redis --workload Chat
```

# Run against Valkey
```sh
./kv_benchmark/target/maxperf/kv_benchmark --db Valkey --workload Chat
```

Example 3: High-Throughput Pipelined Benchmark
Test maximum WRITE throughput using pipelining with 1 million operations and 1KB JSON values.
      
./kv_benchmark/target/maxperf/kv_benchmark \
  --db Redis \
  --workload Simple \
  --write-only \
  --num-ops 1000000 \
  --value-size 1024 \
  --format json \
  --pipeline \
  --batch-size 100

Example 4: NATS Pub/Sub Benchmark
Test NATS pub/sub performance with 10 publishers and 90 subscribers.
      
./kv_benchmark/target/maxperf/kv_benchmark \
  --db Nats \
  --pubsub-only \
  --num-ops 100000 \
  --concurrency 100 \
  --num-publishers 10

Running the WebSocket Benchmark

The WebSocket benchmark requires running the included Axum-based WebSocket server first.

Step 1: Start the WebSocket Server
In one terminal, run the benchmark executable with the --run-ws-server flag. We specify --db WebSocketChat to ensure it runs with the correct pub/sub logic.

./kv_benchmark/target/maxperf/kv_benchmark --run-ws-server --db WebSocketChat

The server will start on 0.0.0.0:3000.

Step 2: Run the WebSocket Benchmark Client
In another terminal, run the client pointed at the server. This test emulates a chat application, measuring the rate at which messages can be published and broadcast to all clients.

./kv_benchmark/target/maxperf/kv_benchmark \
  --db WebSocketChat \
  --concurrency 100 \
  --num-ops 100000

## Command-Line Options

The following are some of the most common flags. Use `--help` to see the full list.

| Flag              | Default   | Description                                                                                           |
| ----------------- | --------- | ----------------------------------------------------------------------------------------------------- |
| `--db`            | `Redis`   | Database to test. Options: `Redis`, `Valkey`, `RustDb`, `InMemory`, `Nats`, `WebSocketChat`.         |
| `--workload`      | `Chat`    | Workload type. Options: `Simple`, `Chat`.                                                             |
| `--num-ops`       | `100000`  | Total number of operations to perform.                                                               |
| `--concurrency`   | `50`      | Number of concurrent client tasks.                                                                   |
| `--value-size`    | `128`     | Base size of the value in bytes before serialization.                                               |
| `--format`        | `String`  | Serialization format: `String`, `Json`, `Bitcode`, `Protobuf`, `Rkyv`, `Flatbuffers`.                 |
| `--pipeline`      | `false`   | Use explicit pipelining for commands to maximize throughput.                                          |
| `--batch-size`    | `50`      | Batch size for pipelined commands.                                                                   |
| `--compress-zstd` | `false`   | Enable zstd compression for payloads.                                                                |
| `--write-only`    | `false`   | Run only write benchmarks.                                                                           |
| `--read-only`     | `false`   | Run only read benchmarks.                                                                            |
| `--pubsub-only`   | `false`   | Run only Pub/Sub benchmarks.                                                                         |
| `--num-publishers`| `1`       | Number of publisher clients for Pub/Sub benchmarks.                                                  |
| `--no-latency`    | `false`   | Skip latency tracking to reduce overhead.                                                            |
| `--run-ws-server` | `false`   | Run the WebSocket server instead of a benchmark client.                                              |

This project is licensed under the [MIT License](LICENSE).