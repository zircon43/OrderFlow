# OrderFlow: Low-Latency Limit Order Book Engine

OrderFlow is a high-performance, deterministic Limit Order Book (LOB) matching engine built in modern C++. It leverages a lock-free Dual SPSC (Single-Producer Single-Consumer) ring-buffer architecture to decouple the core matching thread from network and persistence overhead.

## Architecture

1. **Binary TCP Ingress**: High-frequency clients connect via non-blocking TCP (using Linux `epoll`) on **port 3000** (default) and send raw 30-byte `Order` structs, bypassing HTTP/JSON overhead.
2. **Core Matching Engine**: Uses a highly optimized limit order book pinned to a dedicated CPU core, achieving sub-microsecond internal matching latencies.
3. **Dual Ring Buffers**: Matches are pushed into two parallel lock-free SPSC queues:
    - **Kafka Producer Queue**: A background C++ thread dequeues trades and streams them asynchronously via HTTP to an out-of-process Node.js Kafka-inspired broker (`minikafka`) on **port 9092** for Write-Ahead Log (WAL) durability.
    - **Gateway Queue**: An embedded PubSubServer running on **port 3001** consumes trades and broadcasts them to real-time WebSockets telemetry clients.

## Performance Limits (Benchmarks)
In isolated CPU microbenchmarks on modern hardware, the pure matching logic executes at:
- **Median (p50):** ~117 ns 
- **p99 Latency:** ~334 ns 

In End-to-End ingestion benchmarks constrained on a 2-core cloud environment (where ingestion, matching, and Kafka persistence compete for CPU time), the pipeline comfortably achieves **140,000+ orders/sec** throughput.

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
node minikafka/main.js
```

### 2. Compile and Start the C++ Engine
To build the main matching engine, use CMake in the `src/core` directory:

```bash
cd src/core
mkdir build && cd build
cmake ..
make

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