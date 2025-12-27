#include "bridge.h"
#include "mpmc_queue.hpp"
#include "worker.hpp"
#include <thread>
#include <atomic>
#include <iostream>
#include <vector>
#include <memory>
#include <cstring>

// Define the request type used in the queue
struct Request {
    int64_t sku_id;
    int32_t qty;
    int32_t _pad1;
    uint64_t guest_id;
    uint64_t request_id;
};

// Ensure layout compatibility
static_assert(sizeof(Request) == sizeof(CSeckillRequest), "Request and CSeckillRequest size mismatch");

// Global Engine State
static std::unique_ptr<MpmcQueue<Request>> queue;
static std::unique_ptr<MpmcQueue<CSeckillResult>> result_queue;
static std::atomic<bool> running{false};
static std::thread dispatcher_thread;

// Worker Pool
static std::vector<std::unique_ptr<Worker>> workers;
static const int kWorkerCount = 4; // Configurable

// Global Stats
static std::atomic<uint64_t> sold_total{0};
// Sold Out Flags (Shared State)
static std::unordered_map<int64_t, std::unique_ptr<std::atomic<bool>>> sold_out_store;

// Correctness Check State (Still kept for SPSC verification)
static std::atomic<bool> check_correctness{false};
static uint64_t expected_seq = 1;
static uint64_t gap_count = 0;
static uint64_t dup_count = 0;
static uint64_t last_seq_seen = 0;

std::atomic<uint64_t> barrier_reached_count{0};
std::atomic<uint64_t> target_barrier_seq{0};

void DispatcherLoop() {
    // Batch buffer
    const size_t kBatchSize = 128;
    Request batch[kBatchSize];

    Backoff backoff;

    while (running.load(std::memory_order_relaxed) || (queue && queue->size() > 0)) {
        // 1. Batch Dequeue from MPMC (L1 Buffer)
        size_t count = 0;
        for (size_t i = 0; i < kBatchSize; ++i) {
            if (queue->try_dequeue(batch[i])) {
                count++;
            } else {
                break;
            }
        }
        
        if (count > 0) {
            backoff.reset();
            
            for (size_t i = 0; i < count; ++i) {
                const auto& req = batch[i];

                // Check for Barrier
                if (req.request_id == UINT64_MAX) {
                    target_barrier_seq.store(req.guest_id); // Use guest_id as barrier seq
                    barrier_reached_count.store(0);
                    
                    WorkerRequest wreq;
                    std::memcpy(&wreq, &req, sizeof(WorkerRequest));
                    
                    for (int w = 0; w < kWorkerCount; ++w) {
                        while (!workers[w]->Enqueue(wreq)) {
                            std::this_thread::yield();
                             if (!running.load(std::memory_order_relaxed)) break;
                        }
                    }
                    continue;
                }

                // SPSC Correctness Check (Optional)
                if (check_correctness.load(std::memory_order_relaxed)) {
                    if (req.request_id == expected_seq) {
                        expected_seq++;
                    } else if (req.request_id < expected_seq) {
                        dup_count++;
                    } else {
                        gap_count += (req.request_id - expected_seq);
                        expected_seq = req.request_id + 1;
                    }
                    last_seq_seen = req.request_id;
                }

                // 2. Dispatch to Worker (Load Balancing / Sharding)
                // Sharding by SKU ID to ensure thread-local inventory safety
                // Or sharding by User ID if user-centric limits are needed
                // Here we shard by SKU ID
                int worker_idx = std::abs((long)req.sku_id) % kWorkerCount;
                
                // Convert Request to WorkerRequest (memcpy safe due to layout check)
                WorkerRequest wreq;
                std::memcpy(&wreq, &req, sizeof(WorkerRequest));

                // Try enqueue to worker's MPMC queue
                // If full, we implement a simple backpressure (drop or spin)
                // For high throughput, we spin briefly then drop? 
                // Let's spin for now to ensure delivery
                while (!workers[worker_idx]->Enqueue(wreq)) {
                    // Backpressure strategy:
                    // 1. Spin (Blocking dispatcher, backpressure propagates to SPSC)
                    // 2. Drop (Lossy)
                    // Here we choose blocking to favor correctness
                     std::this_thread::yield();
                     if (!running.load(std::memory_order_relaxed)) break;
                }
            }
        } else {
            backoff.pause();
        }
    }
}

void InitEngine() {
    if (running.load()) return;

    // Reset stats
    sold_total = 0;
    expected_seq = 1;
    gap_count = 0;
    dup_count = 0;
    last_seq_seen = 0;

    // 1. Initialize MPMC Queue (Input)
    // Reduce to 1024 for Backpressure Test
    queue = std::make_unique<MpmcQueue<Request>>(1024);
    
    // 1.5 Initialize Result Queue (MPMC)
    // Reduce to 1024 for Backpressure Test
    result_queue = std::make_unique<MpmcQueue<CSeckillResult>>(1024);

    // Initialize Sold Out Flags
    sold_out_store.clear();
    sold_out_store[123] = std::make_unique<std::atomic<bool>>(false);
    sold_out_store[456] = std::make_unique<std::atomic<bool>>(false);
    sold_out_store[999] = std::make_unique<std::atomic<bool>>(false);
    sold_out_store[888] = std::make_unique<std::atomic<bool>>(false);

    // Create map for workers (raw pointers)
    std::unordered_map<int64_t, std::atomic<bool>*> worker_flags;
    for (auto& kv : sold_out_store) {
        worker_flags[kv.first] = kv.second.get();
    }

    // 2. Initialize Workers
    workers.clear();
    for (int i = 0; i < kWorkerCount; ++i) {
        workers.push_back(std::make_unique<Worker>(i, sold_total, result_queue.get(), &barrier_reached_count, worker_flags));
        workers[i]->Start();
    }

    running.store(true);
    
    // 3. Start Dispatcher
    dispatcher_thread = std::thread(DispatcherLoop);
    
    std::cout << "[C++ Engine] Started. Queue Capacity: 65536" 
              << ", Workers: " << kWorkerCount << std::endl;
}

int EnqueueRequest(CSeckillRequest creq) {
    if (!running.load(std::memory_order_relaxed)) return 0;

    Request req;
    req.sku_id = creq.sku_id;
    req.qty = creq.qty;
    req.guest_id = creq.guest_id;
    req.request_id = creq.request_id;

    if (req.request_id > 100000) {
        std::cout << "[Bridge Error] EnqueueRequest Suspicious ID: " << req.request_id << std::endl;
    }

    // Fast-Path: Check Sold Out Flag
    auto it = sold_out_store.find(req.sku_id);
    if (it != sold_out_store.end()) {
        if (it->second->load(std::memory_order_acquire)) {
            return 2; // Sold Out
        }
    }

    if (queue->try_enqueue(req)) {
        return 1;
    }
    return 0; // Queue Full
}

int EnqueueBatch(CSeckillRequest* reqs, int count) {
    if (!running.load(std::memory_order_relaxed)) return 0;
    
    // Safety cast: CSeckillRequest and Request have identical layout
    const Request* first = reinterpret_cast<const Request*>(reqs);
    
    int enqueued = 0;
    for (int i = 0; i < count; ++i) {
        if (queue->try_enqueue(first[i])) {
            enqueued++;
        } else {
            break; // Stop if full
        }
    }
    return enqueued;
}

void WaitEngineDrained() {
    static std::atomic<bool> inside{false};
    if (inside.exchange(true)) {
        std::cout << "[C++ Engine] WaitEngineDrained re-entered! Ignoring." << std::endl;
        return;
    }

    std::cout << "[C++ Engine] Initiating Barrier Drain... Queue Size: " << (queue ? queue->size() : 0) << std::endl;
    
    // 1. Reset Barrier Counter (Safety, though Dispatcher also does it)
    barrier_reached_count.store(0);
    
    // 2. Inject Barrier Request
    Request barrier_req;
    std::memset(&barrier_req, 0, sizeof(barrier_req));
    barrier_req.request_id = UINT64_MAX; 
    
    // Spin until enqueued (since this is critical for shutdown)
    // If queue is full, we must wait.
    while (!queue->try_enqueue(barrier_req)) {
        std::this_thread::yield();
    }
    std::cout << "[C++ Engine] Barrier Request Enqueued." << std::endl;

    // 3. Wait for all workers to hit the barrier
    // We expect kWorkerCount acks.
    int last_count = -1;
    int stuck_counter = 0;

    while (true) {
        uint64_t current = barrier_reached_count.load(std::memory_order_acquire);
        
        if (current >= kWorkerCount) {
            break;
        }

        if (!running.load(std::memory_order_relaxed)) {
             std::cout << "[C++ Engine] Engine stopped while waiting for barrier." << std::endl;
             break;
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        
        // Debug logging if stuck
        if (current == last_count) {
            stuck_counter++;
            if (stuck_counter % 100 == 0) { // Every 1s
                std::cout << "[C++ Engine] Waiting for barrier... Reached: " << current << "/" << kWorkerCount << std::endl;
            }
        } else {
            last_count = current;
            stuck_counter = 0;
        }
    }
    
    std::cout << "[C++ Engine] Barrier Reached. Engine Drained." << std::endl;
    inside.store(false);
}

void WaitBarrier(uint64_t seq) {
    WaitEngineDrained();
}

void RequestStop() {
    running.store(false);
    if (dispatcher_thread.joinable()) {
        dispatcher_thread.join();
    }
    std::cout << "[C++ Engine] RequestStop completed (Dispatcher stopped)." << std::endl;
}

void JoinEngine() {
    uint64_t total_processed = 0;
    for (auto& worker : workers) {
        worker->Stop();
        uint64_t p = worker->GetProcessedCount();
        total_processed += p;
        std::cout << "[C++ Engine] Worker " << worker->GetId() << " processed: " << p << std::endl;
    }
    workers.clear();
    std::cout << "[C++ Engine] JoinEngine completed. Total Processed: " << total_processed << std::endl;
}

void StopEngine() {
    RequestStop();
    JoinEngine();
}

uint64_t GetSoldTotal() {
    return sold_total.load(std::memory_order_relaxed);
}

uint64_t GetQueueSize() {
    if (queue) {
        return queue->size();
    }
    return 0;
}

uint64_t GetPendingCount() {
    uint64_t total = 0;
    if (queue) {
        total += queue->size();
    }
    for (auto& worker : workers) {
        if (worker) {
            total += worker->GetQueueSize();
        }
    }
    return total;
}

int IsEngineIdle() {
    // 1. Check Input Queue (Dispatcher SPSC)
    if (queue && queue->size() > 0) return 0;
    
    // 2. Check Workers (Queue Size + Processing Flag)
    for (auto& worker : workers) {
        if (worker) {
            if (worker->GetQueueSize() > 0) return 0;
            if (worker->IsProcessing()) return 0;
        }
    }
    
    // 3. Check Output Queue
    if (result_queue && result_queue->size() > 0) return 0;
    
    return 1;
}

void GetEngineStatus(uint64_t* pending_input, uint64_t* active_workers, uint64_t* pending_output) {
    if (queue) *pending_input = queue->size();
    else *pending_input = 0;
    
    *active_workers = 0;
    for (auto& worker : workers) {
        if (worker) {
            *pending_input += worker->GetQueueSize(); // Add worker queues to pending input concept
            if (worker->IsProcessing()) (*active_workers)++;
        }
    }
    
    if (result_queue) *pending_output = result_queue->size();
    else *pending_output = 0;
}

void EnableCorrectnessCheck(int enabled) {
    check_correctness.store(enabled != 0, std::memory_order_relaxed);
    if (enabled) {
        // Reset counters when enabling
        expected_seq = 1;
        gap_count = 0;
        dup_count = 0;
        last_seq_seen = 0;
    }
}

void GetConsumerStats(uint64_t* last_seq, uint64_t* gap, uint64_t* dup) {
    if (last_seq) *last_seq = last_seq_seen;
    if (gap) *gap = gap_count;
    if (dup) *dup = dup_count;
}

int PollResult(CSeckillResult* res) {
    if (result_queue && result_queue->try_dequeue(*res)) {
        return 1;
    }
    return 0;
}
