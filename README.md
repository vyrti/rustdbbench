# Redis vs. Valkey Benchmark

A high-performance, configurable benchmarking tool written in Rust to compare the performance of Redis and Valkey.

This tool is designed to measure throughput (ops/sec) and latency for basic `GET`/`SET` commands while testing the impact of different data serialization formats.

## About

When choosing a key-value store, performance is a critical factor. This project provides a flexible benchmark utility to:
- Compare the performance of Redis and its community-driven fork, Valkey.
- Analyze the overhead of different serialization formats (e.g., JSON vs. Protobuf).
- Simulate various workloads by adjusting concurrency, data size, and command batching (pipelining).

## Features

- **Side-by-Side Benchmarking**: Runs the same workload against both Redis and Valkey for direct comparison.
- **Multiple Serialization Formats**: Natively supports:
  - `String` (raw bytes)
  - `JSON` (`serde_json`)
  - `Bitcode` (a compact binary format)
  - `Protocol Buffers` (`prost`)
  - `Rkyv` (for zero-copy deserialization)
- **Configurable Workloads**: Adjust key/value sizes, number of operations, and concurrency level.
- **Pipelining Support**: Measure maximum throughput by batching commands in a pipeline.
- **Latency Metrics**: Optional latency tracking provides average and p99 latency percentiles (not available in pipelined mode).
- **Zero-Copy Simulation**: A special `zero-copy-read` mode measures raw server/network throughput by skipping the client-side deserialization cost.

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

2.  **Start Databases**
    Use Docker Compose to spin up Redis and Valkey instances. Redis will be available on port `6379` and Valkey on `6380`.
    ```sh
    docker-compose up -d
    ```
    You can check if they are running with `docker ps`.

3.  **Build the Benchmark Tool**
    A build script is provided to compile the project with performance optimizations.
    ```sh
    bash build.sh
    ```
    The optimized executable will be located at `kv_benchmark/target/maxperf/kv_benchmark`.

## Usage

### Command-Line Options

The tool is configured via command-line arguments.

| Flag                 | Alias | Default                  | Description                                                                    |
| -------------------- | ----- | ------------------------ | ------------------------------------------------------------------------------ |
| `--redis-url`        | `-r`  | `redis://127.0.0.1:6379` | Connection URL for Redis.                                                      |
| `--valkey-url`       | `-v`  | `redis://127.0.0.1:6380` | Connection URL for Valkey.                                                     |
| `--num-ops`          | `-n`  | `100000`                 | Total number of operations to run.                                             |
| `--concurrency`      | `-c`  | `50`                     | Number of concurrent client tasks.                                             |
| `--key-size`         |       | `16`                     | Size of the keys in bytes.                                                     |
| `--value-size`       |       | `128`                    | Base size of the value in bytes before serialization.                          |
| `--format`           |       | `string`                 | Data serialization format. Options: `string`, `json`, `bitcode`, `protobuf`, `rkyv`. |
| `--write-only`       |       | `false`                  | Run only write benchmarks.                                                     |
| `--read-only`        |       | `false`                  | Run only read benchmarks.                                                      |
| `--pipeline`         |       | `false`                  | Use explicit pipelining for commands to maximize throughput.                   |
| `--batch-size`       |       | `50`                     | Batch size for pipelined commands.                                             |
| `--no-latency`       |       | `false`                  | Skip latency tracking (auto-disabled for pipeline mode).                       |
| `--zero-copy-read`   |       | `false`                  | Simulate zero-copy reads by skipping/optimizing deserialization.               |

### Examples

**Example 1: Basic Benchmark**

Run a standard benchmark with default settings (100k ops, 50 clients, 128B string values):

```sh
./kv_benchmark/target/maxperf/kv_benchmark
```

---

**Example 2: High-Throughput Pipelined Benchmark**

Test performance with 1 million operations, 100 concurrent clients, 1KB JSON values, and a pipeline batch size of 100:

```sh
./kv_benchmark/target/maxperf/kv_benchmark \
  -n 1000000 \
  -c 100 \
  --value-size 1024 \
  --format json \
  --pipeline \
  --batch-size 100
```
  --batch-size 100
IGNORE_WHEN_COPYING_START
content_copy
download
Use code with caution.
Sh
IGNORE_WHEN_COPYING_END

Example 3: Test Rkyv Zero-Copy Read Performance

Isolate and measure read performance using the rkyv format with zero-copy reads enabled. This test will pre-populate the databases before running the read benchmark.

Generated sh
./kv_benchmark/target/maxperf/kv_benchmark --read-only --format rkyv --zero-copy-read
IGNORE_WHEN_COPYING_START
content_copy
download
Use code with caution.
Sh
IGNORE_WHEN_COPYING_END
Sample Benchmark Results

Note: The following results were captured on a specific machine and are for illustrative purposes. Your mileage will vary based on hardware and workload. All tests were run with -n 100000 -c 100 --pipeline.

Test 1: bitcode format, 10KB values

--format bitcode --value-size 10000

Database	Op Type	Ops/sec	Total Time(s)
Redis	WRITE	70,777.70	1.413
Redis	READ	145,138.62	0.689
Valkey	WRITE	75,242.10	1.329
Valkey	READ	140,158.16	0.713
Test 2: json format, 10KB values

--format json --value-size 10000

Database	Op Type	Ops/sec	Total Time(s)
Redis	WRITE	34,049.54	2.937
Redis	READ	90,777.11	1.102
Valkey	WRITE	37,146.46	2.692
Valkey	READ	107,253.44	0.932
Test 3: protobuf format, 10KB values, zero-copy read

--format protobuf --value-size 10000 --zero-copy-read

Database	Op Type	Ops/sec	Total Time(s)
Redis	WRITE	73,817.73	1.355
Redis	READ (Zero-Copy)	132,707.57	0.754
Valkey	WRITE	86,961.77	1.150
Valkey	READ (Zero-Copy)	136,438.13	0.733

License

This project is licensed under the MIT License. See the LICENSE file for details.