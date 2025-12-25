#include "bridge.h"
#include "spsc_queue.hpp"
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
static std::unique_ptr<SpscQueue<Request>> queue;
static std::unique_ptr<MpmcQueue<CSeckillResult>> result_queue;
static std::atomic<bool> running{false};
static std::thread dispatcher_thread;

// Worker Pool
static std::vector<std::unique_ptr<Worker>> workers;
static const int kWorkerCount = 4; // Configurable

// Global Stats
static std::atomic<uint64_t> sold_total{0};

// Correctness Check State (Still kept for SPSC verification)
static std::atomic<bool> check_correctness{false};
static uint64_t expected_seq = 1;
static uint64_t gap_count = 0;
static uint64_t dup_count = 0;
static uint64_t last_seq_seen = 0;

void DispatcherLoop() {
    // Batch buffer
    const size_t kBatchSize = 128;
    Request batch[kBatchSize];

    Backoff backoff;

    while (running.load(std::memory_order_relaxed)) {
        // 1. Batch Dequeue from SPSC (L1 Buffer)
        size_t count = queue->dequeue_bulk(batch, kBatchSize);
        
        if (count > 0) {
            backoff.reset();
            
            for (size_t i = 0; i < count; ++i) {
                const auto& req = batch[i];

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
                int worker_idx = req.sku_id % kWorkerCount;
                
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

    // 1. Initialize SPSC Queue
    queue = std::make_unique<SpscQueue<Request>>(1024 * 64); 
    
    // 1.5 Initialize Result Queue (MPMC)
    result_queue = std::make_unique<MpmcQueue<CSeckillResult>>(1024 * 64);

    // 2. Initialize Workers
    workers.clear();
    for (int i = 0; i < kWorkerCount; ++i) {
        workers.push_back(std::make_unique<Worker>(i, sold_total, result_queue.get()));
        workers[i]->Start();
    }

    running.store(true);
    
    // 3. Start Dispatcher
    dispatcher_thread = std::thread(DispatcherLoop);
    
    std::cout << "[C++ Engine] Started. Queue Capacity: " << queue->capacity() 
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

    if (queue->try_enqueue(req)) {
        return 1;
    }
    return 0; // Queue Full
}

int EnqueueBatch(CSeckillRequest* reqs, int count) {
    if (!running.load(std::memory_order_relaxed)) return 0;
    
    // Safety cast: CSeckillRequest and Request have identical layout
    const Request* first = reinterpret_cast<const Request*>(reqs);
    return (int)queue->enqueue_bulk(first, (size_t)count);
}

void WaitEngineDrained() {
    std::cout << "[C++ Engine] Waiting for engine to drain..." << std::endl;
    int idle_streak = 0;
    while (true) {
        bool busy = false;
        
        // 1. Check Input Queue
        if (queue && queue->size() > 0) {
            busy = true;
        }

        // 2. Check Workers
        if (!busy) {
            for (auto& worker : workers) {
                if (worker->GetQueueSize() > 0 || worker->IsProcessing()) {
                    busy = true;
                    break;
                }
            }
        }

        if (!busy) {
            // Require 5 consecutive idle checks to be sure (debounce)
            idle_streak++;
            if (idle_streak >= 5) {
                break;
            }
        } else {
            idle_streak = 0;
            // Debug Log every 50 iterations (0.5 sec approx)
            static int loop_count = 0;
            if (++loop_count % 50 == 0) {
                std::cout << "[C++ Engine Debug] Busy. InputQueue: " << (queue ? queue->size() : 0);
                for(auto& w : workers) {
                    std::cout << " W" << w->GetId() << "(Q:" << w->GetQueueSize() << ",P:" << w->IsProcessing() << ")";
                }
                std::cout << std::endl;
            }
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    std::cout << "[C++ Engine] Drained." << std::endl;
}

void StopEngine() {
    running.store(false);
    if (dispatcher_thread.joinable()) {
        dispatcher_thread.join();
    }
    
    uint64_t total_processed = 0;
    for (auto& worker : workers) {
        worker->Stop();
        uint64_t p = worker->GetProcessedCount();
        total_processed += p;
        std::cout << "[C++ Engine] Worker " << worker->GetId() << " processed: " << p << std::endl;
    }
    workers.clear();

    std::cout << "[C++ Engine] Stopped. Total Processed: " << total_processed << std::endl;
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
