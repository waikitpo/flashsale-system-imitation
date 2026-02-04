#pragma once

#include <thread>
#include <atomic>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <chrono>
#include <iostream>
#include <memory>
#include <cstring> // For std::memset

#if defined(_MSC_VER) && (defined(_M_IX86) || defined(_M_X64))
#include <immintrin.h>
#elif defined(__i386__) || defined(__x86_64__)
#include <immintrin.h>
#endif

#include "bridge.h" // For CSeckillRequest/Request struct
#include "mpmc_queue.hpp"
#include "spsc_queue.hpp"
#include "wal.hpp"

// Simple backoff strategy for spinning
class Backoff {
public:
    void pause() noexcept {
        if (spins_ < kSpinLimit) {
            ++spins_;
#if defined(_MSC_VER) && (defined(_M_IX86) || defined(_M_X64))
            _mm_pause();
#elif defined(__i386__) || defined(__x86_64__)
            _mm_pause();
#endif
        } else {
            std::this_thread::yield();
        }
    }

    void reset() noexcept { spins_ = 0; }

private:
    static constexpr int kSpinLimit = 64;
    int spins_{0};
};

// Re-definition of Request struct if not available globally, 
// but we should reuse the one in bridge.cpp context if possible.
struct WorkerRequest {
    int64_t sku_id;
    int32_t qty;
    int32_t _pad1;
    uint64_t guest_id;
    uint64_t request_id;
    // Latency Tracking
    int64_t ts_ingress;
    int64_t ts_pop_mpmc;
    int64_t ts_push_spsc;
    int64_t ts_pop_spsc;
};

class Worker {
public:
    Worker(int id, std::atomic<uint64_t>& global_sold_counter, MpmcQueue<CSeckillResult>* result_queue, std::atomic<uint64_t>* barrier_count, 
           std::unordered_map<int64_t, std::atomic<bool>*> sold_out_flags) 
        : id_(id), sold_total_(global_sold_counter), running_(false), result_queue_(result_queue), barrier_count_(barrier_count), sold_out_flags_(sold_out_flags),
          check_correctness_(nullptr), last_seq_(nullptr), gap_(nullptr), dup_(nullptr) {
        // Initialize mock inventory
        inventory_[123] = 10000000; // ample stock for testing
        inventory_[456] = 100;
        inventory_[666] = 100; // Cluster Verification Test
        inventory_[999] = 100; // Overselling test: 100 items
        inventory_[888] = 5;   // Flag Logic Regression Test: 5 items
        inventory_[777] = 50000; // Benchmark Latency Test

        // Initialize Multi-SKU Benchmark items
        for (int64_t i = 1001; i <= 1008; ++i) {
            inventory_[i] = 5000;
        }
        
        // SPSC queue for this worker (Dispatcher -> Worker is 1:1)
        // Capacity 16K
        queue_ = std::make_unique<SpscQueue<WorkerRequest>>(16384);

        // Initialize WAL
        // Ensure "data" directory exists in the running directory (backend/)
        wal_path_ = "data/worker_" + std::to_string(id) + ".wal";
        wal_ = std::make_unique<WalLogger>(wal_path_);
    }

    void Start() {
        running_.store(true, std::memory_order_release);
        thread_ = std::thread(&Worker::Loop, this);
    }

    void Stop() {
        running_.store(false, std::memory_order_release);
        if (thread_.joinable()) {
            thread_.join();
        }
        // Flush WAL on stop
        if (wal_) {
            wal_->Close();
        }
    }

    size_t GetQueueSize() const {
        if (queue_) return queue_->size();
        return 0;
    }

    bool IsProcessing() const {
        return is_processing_.load(std::memory_order_acquire);
    }

    // Dispatcher calls this to push request to worker
    bool Enqueue(const WorkerRequest& req) {
        return queue_->try_enqueue(req);
    }

    uint64_t GetProcessedCount() const {
        return processed_count_.load(std::memory_order_acquire);
    }

    int GetId() const { return id_; }

private:
    // Barrier Counter Reference
    std::string wal_path_;
    std::atomic<uint64_t>* barrier_count_;
    
    // Correctness Check Pointers (Optional)
    std::atomic<bool>* check_correctness_;
    uint64_t* last_seq_;
    uint64_t* gap_;
    uint64_t* dup_;

    void Loop() {
        // Pin to core?
        // SetThreadAffinity(id_);
        
        const size_t kBatchSize = 64;
        WorkerRequest batch_buffer[kBatchSize];

        for (;;) {
            size_t count = queue_->try_dequeue_batch(batch_buffer, kBatchSize);
            
            if (count > 0) {
                // Record SPSC Pop Time for the batch
                int64_t now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now().time_since_epoch()).count();

                is_processing_.store(true, std::memory_order_release);
                for (size_t i = 0; i < count; ++i) {
                    batch_buffer[i].ts_pop_spsc = now;
                    Process(batch_buffer[i]);
                }
                is_processing_.store(false, std::memory_order_release);
                processed_count_.fetch_add(count, std::memory_order_release);
            } else {
                if (!running_.load(std::memory_order_acquire)) {
                    // Double check queue is empty
                    count = queue_->try_dequeue_batch(batch_buffer, kBatchSize);
                    if (count == 0) {
                        break;
                    }
                    // Record SPSC Pop Time for the batch
                    int64_t now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now().time_since_epoch()).count();

                    is_processing_.store(true, std::memory_order_release);
                    for (size_t i = 0; i < count; ++i) {
                        batch_buffer[i].ts_pop_spsc = now;
                        Process(batch_buffer[i]);
                    }
                    is_processing_.store(false, std::memory_order_release);
                    processed_count_.fetch_add(count, std::memory_order_release);
                    continue;
                }

                // Spin/Yield
                Backoff backoff;
                backoff.pause();
            }
        }
    }

    void Process(const WorkerRequest& req) {
        // 0. WarmUp No-Op
        if (req.request_id == 0) {
            return;
        }

        // 0. Barrier Check
        if (req.request_id == UINT64_MAX) {
            if (barrier_count_) {
                barrier_count_->fetch_add(1, std::memory_order_release);
            }
            return;
        }

        // 0.1 Pure Queue Benchmark Mode
        // If request_id is very large (e.g. > 10^18), we treat it as pure queue benchmark
        // and skip inventory/WAL logic to measure pure MPMC+SPSC overhead.
        if (req.request_id > 18000000000000000000ULL) {
            // Minimal processing to allow Result Queue measurement if needed
             CSeckillResult res;
             // ... minimal setup ...
             res.request_id = req.request_id;
             res.status = 1; 
             // Calculate Latencies
             if (req.ts_ingress > 0) {
                 res.mpmc_latency_ns = req.ts_pop_mpmc - req.ts_ingress;
                 res.spsc_latency_ns = req.ts_pop_spsc - req.ts_push_spsc;
             }
             // Notify Result
             while (!result_queue_->try_enqueue(res)) {
                 Backoff backoff;
                 backoff.pause();
             }
             return;
        }

        // 1. Idempotency Check
        if (IsDuplicate(req.request_id)) {
            return; 
        }

        // 1.5 Correctness Check (For Tests)
        if (check_correctness_ && check_correctness_->load(std::memory_order_relaxed)) {
            if (last_seq_ && gap_ && dup_) {
                uint64_t seq = req.request_id;
                // Initialize if first
                if (*last_seq_ == 0 && seq == 1) {
                    *last_seq_ = 1;
                } else {
                    if (seq > *last_seq_) {
                         if (seq != *last_seq_ + 1) {
                             // Gap detected
                             // std::cout << "Gap: " << *last_seq_ << " -> " << seq << std::endl;
                             *gap_ += (seq - *last_seq_ - 1);
                         }
                         *last_seq_ = seq;
                    } else {
                         // Duplicate or Reordering
                         // std::cout << "Dup: " << seq << " vs " << *last_seq_ << std::endl;
                         *dup_ += 1;
                    }
                }
            }
        }

        // Prepare Result
        CSeckillResult res;
        std::memset(&res, 0, sizeof(res));
        res.request_id = req.request_id;
        res.sku_id = req.sku_id;
        res.qty = req.qty;
        res.guest_id = req.guest_id;
        res.status = 0; // Default: Failed
        
        // Calculate Latencies (in nanoseconds)
        if (req.ts_ingress > 0) {
            res.mpmc_latency_ns = req.ts_pop_mpmc - req.ts_ingress;
            res.spsc_latency_ns = req.ts_pop_spsc - req.ts_push_spsc;
            // Debug fields
            res.ts_ingress = req.ts_ingress;
            res.ts_pop_mpmc = req.ts_pop_mpmc;
        } else {
            res.mpmc_latency_ns = 0;
            res.spsc_latency_ns = 0;
            res.ts_ingress = 0;
            res.ts_pop_mpmc = 0;
        }

        // 2. Inventory Check & Deduct
        auto it = inventory_.find(req.sku_id);
        if (it != inventory_.end()) {
            if (it->second >= req.qty) {
                it->second -= req.qty;
                sold_total_.fetch_add(req.qty, std::memory_order_relaxed);
                
                // Check if sold out after deduction
                if (it->second == 0) {
                    auto flag_it = sold_out_flags_.find(req.sku_id);
                    if (flag_it != sold_out_flags_.end()) {
                        flag_it->second->store(true, std::memory_order_release);
                    }
                }

                // 3. Write to WAL (Only on success)
                WriteToWal(req);

                res.status = 1; // Success
            } else {
                res.status = 2; // Sold Out / Not Enough Stock
                
                // CRITICAL FIX: Only set global Sold Out flag if inventory is EXACTLY zero.
                // If we have 5 items but request 10, we fail this request but MUST NOT blocking future requests for 1 item.
                if (it->second == 0) {
                    auto flag_it = sold_out_flags_.find(req.sku_id);
                    if (flag_it != sold_out_flags_.end()) {
                        flag_it->second->store(true, std::memory_order_release);
                    }
                }
            }
        }

        // 4. Notify Result (Success or Failure)
        Backoff backoff;
        while (!result_queue_->try_enqueue(res)) {
            backoff.pause();
        }
    }

    bool IsDuplicate(uint64_t req_id) {
        if (seen_requests_.find(req_id) != seen_requests_.end()) {
            return true;
        }
        seen_requests_.insert(req_id);
        
        // Cleanup to prevent OOM in long run
        if (seen_requests_.size() > 100000) {
            seen_requests_.clear(); 
        }
        return false;
    }

    void WriteToWal(const WorkerRequest& req) {
        // Skip WAL for pure queue benchmark if req_id indicates benchmark
        // We can use a special flag or just always skip if we want to measure pure queue overhead.
        // But for "realistic" queue benchmark, maybe we should keep it?
        // The user asked for "Queue (MPMC+SPSC) overhead", usually implying exclusion of business logic like WAL.
        // Let's optimize: if WalLogger is null or closed, don't write.
        if (!wal_) return;

        WalRecord record;
        record.timestamp_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        record.sku_id = req.sku_id;
        record.qty = req.qty;
        record.request_id = req.request_id;
        record.guest_id = req.guest_id;

        wal_->Append(&record, sizeof(record));
    }

    int id_;
    std::atomic<uint64_t>& sold_total_;
    std::atomic<bool> running_;
    std::atomic<bool> is_processing_{false};
    std::thread thread_;
    std::unique_ptr<SpscQueue<WorkerRequest>> queue_;
    std::unique_ptr<WalLogger> wal_;
    MpmcQueue<CSeckillResult>* result_queue_;
    
    // Thread-local state
    std::unordered_map<int64_t, int> inventory_;
    std::unordered_set<uint64_t> seen_requests_;
    std::atomic<uint64_t> processed_count_{0};
    std::unordered_map<int64_t, std::atomic<bool>*> sold_out_flags_;
};
