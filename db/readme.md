# In-Memory DB Server

This is a simple, high-performance, in-memory key-value database server built with Rust and Tokio. It listens on a TCP port and communicates using a basic, Redis-like text protocol (RESP).

## Features

- **In-Memory:** All data is stored in RAM for maximum speed.
- **Concurrent:** Uses `tokio` to handle multiple client connections concurrently.
- **Sharded:** The internal data store is sharded to reduce lock contention.
- **Supported Commands:** `GET`, `SET`, `LPUSH`, `LRANGE`.

## Building and Running

1.  **Build the server:**
    ```sh
    cargo build --release
    ```

2.  **Run the server:**
    The server will start and listen on the default address `127.0.0.1:7878`.
    ```sh
    ./target/release/in_memory_db_server
    ```
    You can specify a different address and port using the `--addr` flag:
    ```sh
    ./target/release/in_memory_db_server --addr 0.0.0.0:8000
    ```

## Interacting with the Server

You can use a simple TCP client like `netcat` (`nc`) or `telnet` to interact with the server.

**Open a connection:**
```sh
netcat 127.0.0.1 7878