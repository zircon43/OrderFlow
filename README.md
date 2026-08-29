# OrderFlow: Low-Latency Limit Order Book Engine

OrderFlow is a high-performance, deterministic Limit Order Book (LOB) matching engine built in modern C++17. It is specifically engineered to demonstrate absolute mechanical sympathy, achieving sub-microsecond matching latency and robust crash durability even in heavily constrained compute environments.

## 🚀 Key Engineering Achievements

* **Lock-Free Matching at 117ns:** The core engine processes orders using an optimized `std::map<uint64_t, std::deque<Order>>` to enforce strict price-time priority. In isolated microbenchmarks, the pure matching logic executes at a median latency of **117 nanoseconds**.
* **Zero-Parse IPC & Determinism:** Bypassed HTTP/JSON overhead completely. The engine natively reads 30-byte packed binary C-structs off a non-blocking Linux `epoll` TCP socket, achieving true "zero-parse" ingestion. The core matching thread is strictly pinned to a dedicated hardware core (`pthread_setaffinity_np`) to eliminate OS context-switching jitter.
* **Dual Ring-Buffer Decoupling:** Implemented a **Dual SPSC (Single-Producer Single-Consumer) lock-free ring-buffer** architecture. This perfectly isolates the real-time `PubSubServer` WebSocket telemetry gateway from the disk-bound Kafka persistence path. Downstream network or disk I/O spikes are physically incapable of blocking the core matching thread.
* **Hardcore Zero-Dependency Kafka Integration:** Eschewed generic dependencies (like `librdkafka`) to build a custom C++ Kafka Producer from scratch. It natively crafts binary KRaft `Produce API v11` packets and streams them directly to the broker over bare TCP.
* **Defeating the FSync Latency Penalty:** Proved system resilience via a rigorous `kill -9` crash-recovery benchmark. By fanning out persistence to the Dual SPSC asynchronous queues, the system successfully absorbed a massive **41 ms tail-latency penalty** incurred by strict per-event `fsync()` disk calls, absolutely guaranteeing zero data loss during catastrophic process failures while the core engine continued matching unhindered.

---

## Architecture & Concurrency Model

1. **Binary TCP Ingress**: High-frequency clients connect via non-blocking TCP on **port 3000**. Lock-free **MPSC** queues pipe the order flow safely into the matching engine.
2. **Core Matching Engine**: The deterministic heart of the system, pinned to Core 1, processing the Limit Order Book.
3. **Dual Egress Queues**: 
    - **Gateway Queue (Real-Time)**: Broadcasts sub-millisecond market data to active UI clients on **port 3001**.
    - **Kafka Producer Queue (Durability)**: Streams durable WAL persistence to the `minikafka` broker on **port 9092**.

## True End-to-End Performance Limits
*Note: These benchmarks were captured under extreme stress testing on a strictly constrained 2-core environment, simultaneously running 4 producer threads, the Node.js V8 broker, and the full C++ pipeline.*

- **True E2E Ingestion Throughput:** 434,953 orders/sec
- **End-to-End Latency (p50):** 1.65 ms
- **End-to-End Latency (p99):** 5.17 ms

*The 435k orders/sec E2E ceiling includes the full lifecycle: Client OS thread spawning -> TCP loopback handshakes (`connect`) -> `send()` syscalls -> ingress `epoll` parsing -> matching execution -> egress queueing -> PubSub network broadcast.*

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

```bash
# Using g++ directly
g++ -O3 -pthread -std=c++17 src/core/engine.cpp src/core/lob.cpp src/core/main.cpp src/core/pubsub_server.cpp src/core/tcp_server.cpp src/core/kafka_producer.cpp -o matching_engine

# Starts the engine on default TCP port 3000 and PubSub port 3001
./matching_engine
```

### 3. Run Benchmarks

```bash
# Hot-path isolated benchmark
g++ -O3 -pthread -std=c++17 tests/hotpath_benchmark.cpp src/core/lob.cpp -o hotpath_bench
./hotpath_bench

# End-to-End concurrent benchmark client
g++ -O3 -pthread -std=c++17 tests/benchmark_client.cpp -o benchmark_client
./benchmark_client
```