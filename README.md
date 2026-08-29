# OrderFlow: Low-Latency Limit Order Book Engine

OrderFlow is a high-performance, deterministic Limit Order Book (LOB) matching engine built in modern C++17. It leverages a lock-free Dual SPSC (Single-Producer Single-Consumer) ring-buffer architecture to decouple the core matching thread from network and persistence overhead, ensuring microsecond latency and high availability.

## Architecture & Concurrency Model

1. **Binary TCP Ingress**: High-frequency clients connect via non-blocking TCP (using Linux `epoll`) on **port 3000** (default) and send raw 30-byte `Order` structs, bypassing HTTP/JSON overhead. This achieves "zero-parse IPC" for nanosecond ingestion.
2. **Core Matching Engine**: Uses a highly optimized `std::map`-backed limit order book pinned to a dedicated CPU core (`pthread_setaffinity_np`), achieving sub-microsecond internal matching latencies with strict price-time priority.
3. **Dual Ring Buffers (The Decoupling Strategy)**: Matches are pushed into two parallel lock-free SPSC queues to safely handle two entirely different consumers without blocking the core matching thread:
    - **Gateway Queue (Real-Time)**: An embedded `PubSubServer` running on **port 3001** consumes trades and broadcasts them to real-time WebSockets telemetry clients.
    - **Kafka Producer Queue (Durability)**: A background C++ thread dequeues trades and streams them to an out-of-process JavaScript Kafka broker (`minikafka`) on **port 9092** for durable WAL persistence.

### Zero-Dependency Binary Kafka Producer
Instead of relying on heavy dependencies like `librdkafka` or using slow HTTP proxies, the persistence layer utilizes a custom-built, zero-dependency C++ `KafkaProducer`. It manually serializes trades and constructs the raw binary Kafka **Produce Request (API Key 0, Version 11)**, communicating directly with the `minikafka` broker over a raw TCP socket.

## Performance Limits (Benchmarks)

### Isolated Core Latency
In isolated CPU microbenchmarks on modern hardware, the pure matching logic executes at:
- **Median (p50):** ~117 ns 
- **p99 Latency:** ~334 ns 

### End-to-End (E2E) Ingestion
In End-to-End ingestion benchmarks (concurrently reading TCP, matching, and flushing to Kafka and WebSockets):
- **Throughput:** ~2,000,000 orders/sec
- **Latency (p50):** 1.75 ms
- **Latency (p99):** 3.47 ms

## Durability & Crash Recovery
We enforce strict WAL (Write-Ahead Log) durability. A concurrent `kill -9` (kill-mid-run) benchmark was executed to test data safety under simulated power loss:
- **Batched Fsync (OS Cache):** Relying on OS buffering led to massive data loss during a hard crash (only 209 bytes persisted out of thousands of trades).
- **FSync Per Event:** By enforcing synchronous physical `fsync` per trade batch, the system successfully persisted 108,893 bytes mid-crash with **zero data loss**. The massive 41ms tail-latency penalty of the disk write was entirely absorbed by the lock-free Dual SPSC architecture, keeping the core matching thread unblocked.

---

## How to Build and Run

### Prerequisites
- GCC (with C++17 support)
- CMake (3.10+)
- Node.js (v18+)

### 1. Start the Kafka Persistence Broker
The persistence layer relies on a local Node.js broker. We first pre-provision the metadata to ensure strict validation, and then launch the broker.

```bash
# 1. Pre-provision the topics in the WAL (Write-Ahead Log)
node setup_topic.js

# 2. Start the broker (runs on port 9092)
# Tip: Prefix with FSYNC_MODE=1 for strict physical durability
FSYNC_MODE=1 node minikafka/js-broker/app/main.js
```

### 2. Compile and Start the C++ Engine
To build the main matching engine, use CMake in the `src/core` directory (or use raw `g++`):

```bash
# Using g++ directly
g++ -O3 -pthread -std=c++17 src/core/engine.cpp src/core/lob.cpp src/core/main.cpp src/core/pubsub_server.cpp src/core/tcp_server.cpp src/core/kafka_producer.cpp -o matching_engine

# Starts the engine on default TCP port 3000 and PubSub port 3001
./matching_engine
```

### 3. Run Benchmarks
You can also compile and run the provided E2E or hot-path benchmarks:

```bash
# Hot-path isolated benchmark
g++ -O3 -pthread -std=c++17 tests/hotpath_benchmark.cpp src/core/lob.cpp -o hotpath_bench
./hotpath_bench

# End-to-End concurrent benchmark client
g++ -O3 -pthread -std=c++17 tests/benchmark_client.cpp -o benchmark_client
./benchmark_client
```