# High-Performance Hybrid Seckill Engine

[English](README.md) | [中文](README_zh-CN.md)

**Disclaimer**: This project is for entertainment purposes only and is not recommended for production use. It was largely created via AI-assisted programming ("vibe coding"), so code quality and maintainability are not guaranteed.

A high-performance, crash-safe seckill (flash sale) engine built with **Go** (Interface Layer) and **C++** (Core Engine). Designed for extreme concurrency, low latency, and hardware efficiency.

## Key Features

*   **Hybrid Architecture**: Go handles HTTP/Business Logic, C++ handles high-concurrency inventory processing.
*   **Extreme Performance**:
    *   **Lock-Free Queues**: Uses **Vyukov's MPMC** (Multi-Producer Multi-Consumer) and **SPSC** (Single-Producer Single-Consumer) queues for zero-lock communication.
    *   **Zero Contention**: Thread-per-Core architecture minimizes context switching and lock contention.
    *   **Throughput**: Capable of handling **20k+ TPS** on a single node (Benchmark: 500k requests in ~28s).
*   **Reliability**:
    *   **Zero Data Loss**: Graceful shutdown mechanism ensures all in-flight requests are processed and persisted.
    *   **Crash Recovery (WAL)**: Write-Ahead Logging (WAL) ensures data integrity even after `kill -9` or power failure.
    *   **Backpressure**: Adaptive flow control protects the system from overload.

## Architecture

```mermaid
graph TD
    User((User Traffic))
    
    subgraph "Access Layer (Go / Network IO Bound)"
        Nginx[Nginx LB] -->|HTTP/1.1| GoHandler[Go HTTP Handler]
        GoHandler -->|Pre-Check| Redis[(Redis Cache)]
        GoHandler -.->|CGO Call| CppBridge[C++ Bridge]
    end

    subgraph "Core Engine (C++ / CPU Bound / Thread-per-Core)"
        direction TB
        CppBridge -->|Wait-Free Enqueue| MPMC{MPMC Queue}
        MPMC -->|Poll| Dispatcher[Dispatcher Thread]
        
        Dispatcher -->|Hash(sku_id)| Shard1
        Dispatcher -->|Hash(sku_id)| Shard2
        
        subgraph Shard1 [Core 1: Hot SKU Shard]
            Q1[SPSC Queue] --> W1[Worker Thread 1]
            W1 -->|Decrement| Mem1[In-Memory Stock]
            W1 -->|Append| WAL1[WAL File]
        end
        
        subgraph Shard2 [Core 2: Cold SKU Shard]
            Q2[SPSC Queue] --> W2[Worker Thread 2]
            W2 -->|Decrement| Mem2[In-Memory Stock]
            W2 -->|Append| WAL2[WAL File]
        end
    end

    subgraph "Persistence Layer (Go / Disk IO Bound)"
        W1 & W2 -->|Result Ptr| ResultQ[Result Queue]
        ResultQ -->|Batch Collect| GoDBWorker[Go DB Worker]
        GoDBWorker -->|Batch Insert| DB[(PostgreSQL / SQLite)]
    end
    
    classDef go fill:#e1f5fe,stroke:#01579b,stroke-width:2px;
    classDef cpp fill:#ffebee,stroke:#b71c1c,stroke-width:2px;
    classDef db fill:#fff3e0,stroke:#e65100,stroke-width:2px;
    classDef nginx fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px;
    
    class GoHandler,GoConsumer,GoDBWorker go;
    class Dispatcher,W1,W2,MPMC,Q1,Q2,CppBridge cpp;
    class Redis,DB,WAL1,WAL2 db;
    class Nginx nginx;
```

## Performance Benchmark Report

Based on latest tests (2026-02-03).

**Test Environment:**
- **CPU**: AMD Ryzen 7 5800H (8C/16T)
- **Memory**: 32GB DDR4
- **Infrastructure**: Docker Containers (App + Redis + SQLite)
- **Network**: Localhost (Loopback)

### 1. High Concurrency Seckill Scenario
Simulating real-world flash sale traffic with high contention and overselling.
- **Config**: 200,000 Requests | 1,000 Concurrency | 5,000 Stock
- **Throughput (RPS)**: **~21,786 req/s**
- **HTTP Latency**:
  - **P50**: 43.21 ms
  - **P99**: 78.93 ms
- **Consistency**: **100%** (0 Errors, 0 Oversold)
- **Outcome**: 4,900 Enqueued, 195,100 Sold Out

### 2. Low Latency Scenario
Simulating fast ordering with sufficient stock.
- **Config**: 5,000 Requests | 100 Concurrency
- **HTTP P50 Latency**: **8.48 ms**
- **HTTP P99 Latency**: 40.77 ms

### 3. Core Engine Micro-Latency
Thanks to lock-free queues, the C++ core is extremely fast. Bottlenecks are mainly in Network I/O.
- **MPMC Queue (Ingress -> Dispatcher)**: **~341 µs** (0.34 ms)
- **SPSC Queue (Dispatcher -> Worker)**: **~54 µs** (0.05 ms)
- **Conclusion**: Core queuing latency < 0.5ms.

### 4. Database Batching
- **Strategy**: Asynchronous Batch Writes
- **Effect**: Avg Batch Size **~233 items**, Max **1000 items**.
- **Impact**: Compressed 20,000 DB I/O ops into ~85 batch inserts.

### 5. PostgreSQL Benchmark & Mixed Workload
**Environment**: Same as above, but switched to **PostgreSQL 15** (Docker).

#### 5.1 Single SKU High Concurrency
- **Throughput (RPS)**: **~21,222 req/s** (Similar to SQLite, limited by Network/Redis)
- **Latency**: P50 43.80ms | P99 94.47ms
- **DB Performance**: Avg Batch Latency **~21ms** (Slower than embedded SQLite due to network RTT)

#### 5.2 Mixed Workload (Hot/Cold SKUs)
Simulating 8 SKUs with varied popularity under high load.
- **Config**: 400,000 Requests | 8 SKUs (5,000 Stock/SKU) | 1,000 Concurrency
- **Throughput (RPS)**: **~13,581 req/s**
- **Latency**: P50 61.81ms | P99 183.64ms
- **Bottleneck Analysis**: SPSC Queue Latency spiked to ~419ms. This indicates **PostgreSQL Write Speed became the bottleneck**, causing backpressure on worker threads. The system successfully handled the load via backpressure without crashing or data loss.

## Deep Dive: Core Technology

### 1. Custom Thread Model & SMT Optimization
We eschew generic thread pools in favor of a highly customized **Thread-per-Core** model tailored for modern CPU architectures (e.g., Ryzen 5800H).

*   **Physical vs. Logical Cores**:
    *   **C++ Core (Hot Path)**: We allocate a fixed number of workers (e.g., 4 threads) strictly for the inventory engine. These threads are designed to occupy **independent physical cores**, avoiding the resource contention (ALU/FPU/L1 Cache) typical of SMT (Simultaneous Multithreading/Hyper-Threading).
    *   **Go Runtime (IO Path)**: The remaining logical threads (e.g., 12 threads) are left for Go's runtime to handle HTTP parsing, JSON encoding, and DB IO. This leverages SMT to hide IO latency without polluting the cache of the core engine.
*   **Spinning & Backoff**: Worker threads use a sophisticated **Backoff Strategy** (pause instruction -> yield) instead of immediate sleeping. This keeps the CPU pipeline "hot" and avoids expensive kernel-mode context switches during micro-bursts of traffic.

### 2. Zero-Allocation Memory Strategy
To eliminate GC pauses and allocator overhead, we implement a strict **"No-New-in-Loop"** policy.

*   **Pre-allocated Ring Buffers**: All queues (MPMC/SPSC) allocate their entire backing storage at startup. Requests flow through these fixed memory regions without ever triggering `malloc` or `free`.
*   **Stack Batching**: Workers process requests in batches (e.g., 64 items) allocated entirely on the **CPU Stack**. This ensures data locality and zero heap fragmentation.
*   **Slice Reuse (Go)**: The Go-side consumer reuses underlying slice arrays (`batch = batch[:0]`) to minimize pressure on the Go Garbage Collector.

### 3. Advanced Queueing Theory: The Funnel Model
We combine two types of lock-free queues to achieve the best of both worlds:

*   **Level 1: MPMC (The Funnel)**
    *   **Role**: Rapidly ingests requests from hundreds of concurrent Go goroutines.
    *   **Tech**: Based on **Vyukov's Bounded MPMC**. Uses `CAS` (Compare-And-Swap) on sequence numbers.
    *   **Optimization**: Heavy use of **Cache Line Padding** to prevent *False Sharing* between `head` and `tail` cursors on different CPU cores.

*   **Level 2: SPSC (The Pipe)**
    *   **Role**: Distributes work from the Dispatcher to specific Workers (Sharding by SKU).
    *   **Tech**: **Wait-Free Ring Buffer**. Since there is only 1 Writer and 1 Reader, **CAS is eliminated entirely**. Synchronization relies solely on memory barriers (`acquire`/`release` semantics).
    *   **Benefit**: This offers the theoretical limit of inter-thread communication latency.

## Design Decisions

### Why Hybrid Go + C++?
*   **Go**: Productivity king. Handles the "dirty work" of HTTP protocols, JSON parsing, and DB connectivity with its rich ecosystem (Gin, GORM).
*   **C++**: Performance king. Provides the raw pointer manipulation, manual memory management, and thread pinning required for the <10µs latency inventory check.
*   **Bridge**: We use `cgo` but minimize crossing the boundary by batching requests and using lock-free shared memory queues.

### Persistence & Safety
*   **WAL (Write-Ahead Log)**: Modeled after database journals. Every inventory decrement is appended to a file (`O_APPEND`) before memory update.
*   **Crash Recovery**: On startup, the engine replays the WAL to reconstruct the exact in-memory state.
*   **Two-Phase Shutdown**:
    1.  **Drain**: Stop accepting inputs.
    2.  **Verify**: Wait for `Input == Output`.
    3.  **Stop**: Safe process exit.

### Nginx & Multi-Node Traffic Distribution
We introduce Nginx in the test environment primarily to verify traffic distribution and system behavior in a **Multi-Node Deployment** scenario.
*   **Horizontal Scalability**: Using Nginx to distribute traffic across multiple Backend instances to observe linear throughput growth in cluster mode.
*   **Load Balancing**: Simulating basic load balancing to ensure requests are routed evenly or strategically (e.g., IP Hash) to different compute nodes, validating the correctness of the stateless access layer design.

## Quick Start
Assume you have just cloned the repository. Follow these steps to start and test the system.

### 1. Start Infrastructure
Use Docker Compose to launch Redis and PostgreSQL.

```bash
docker-compose up -d
```
*Starts Redis (Port 6380) and PostgreSQL (Port 5432).*

### 2. Build Backend
Enter the `backend` directory and build. Note: This project uses CGO (Mixed C++), ensure your environment supports it.

```bash
cd backend
go mod tidy
go build -o server .
```

### 3. Run Service
Choose between SQLite (Default) or PostgreSQL.

**Option A: SQLite**
```bash
./server
```

**Option B: PostgreSQL**
```bash
# Ensure docker containers are running
USE_PG=true ./server
```
*Service listens on `:3000`.*

### 4. Run Benchmarks
Keep the service running, open a NEW terminal, and run the benchmark scripts.

```bash
cd backend/tests

# Run High Concurrency Benchmark (200k Reqs, 1000 Concurrency)
go run benchmark_high_concurrency.go

# Run Mixed Workload Benchmark (Real-world Simulation)
go run benchmark_multi_sku.go
```

---

## Project Structure

## About the Frontend

Under development.
