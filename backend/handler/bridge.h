#ifndef BRIDGE_H
#define BRIDGE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

typedef struct {
    int64_t sku_id;
    int32_t qty;
    int32_t _pad1;
    uint64_t guest_id;
    uint64_t request_id;
    // Latency Tracking
    int64_t ts_ingress;   // Time when EnqueueRequest called
    int64_t ts_pop_mpmc;  // Time when popped from MPMC (Dispatcher)
    int64_t ts_push_spsc; // Time when pushed to SPSC (Dispatcher)
    int64_t ts_pop_spsc;  // Time when popped from SPSC (Worker)
} CSeckillRequest;

typedef struct {
    int64_t sku_id;
    int32_t qty;
    int32_t _pad1;
    uint64_t guest_id;
    uint64_t request_id;
    int32_t status; // 1=Success, 0=Fail
    int32_t _pad2;
    // Latency Reporting
    int64_t mpmc_latency_ns; // ts_pop_mpmc - ts_ingress
    int64_t spsc_latency_ns; // ts_pop_spsc - ts_push_spsc
    int64_t ts_ingress;      // Time when EnqueueRequest called
    int64_t ts_pop_mpmc;     // Time when popped from MPMC (Dispatcher)
} CSeckillResult;

// Initialize the engine (start consumer thread)
void InitEngine();

// Warm up the engine by sending dummy requests
void WarmUpEngine();

// Enqueue a request. Returns 1 if successful, 0 if queue is full.
int EnqueueRequest(CSeckillRequest req);

// Batch enqueue. Returns number of requests enqueued.
int EnqueueBatch(CSeckillRequest* reqs, int count);

// Enqueue a barrier with a sequence number
void EnqueueBarrier(uint64_t seq);

// Wait until engine is fully drained (Input Empty + Workers Idle)
void WaitEngineDrained();

// Wait for a Barrier Event with sequence number
// Blocks until all requests <= seq are processed and results enqueued.
void WaitBarrier(uint64_t seq);

// Request engine stop (set flag, stop dispatcher)
void RequestStop();

// Join engine threads (wait for workers)
void JoinEngine();

// Stop the engine (RequestStop + JoinEngine)
void StopEngine();

// Get total sold count (for stats)
uint64_t GetSoldTotal();

// Get current queue size (approximate)
uint64_t GetQueueSize();

// Get total pending requests (SPSC + Workers)
uint64_t GetPendingCount();

// Check if engine is completely idle (Input Queue Empty + All Workers Idle + Output Queue Empty)
int IsEngineIdle();

// Get detailed engine status
void GetEngineStatus(uint64_t* pending_input, uint64_t* active_workers, uint64_t* pending_output);

// Enable correctness check mode
void EnableCorrectnessCheck(int enabled);

// Get correctness stats
void GetConsumerStats(uint64_t* last_seq, uint64_t* gap_count, uint64_t* dup_count);

// Get internal queue counters (enqueue/dequeue attempts)
void GetQueueCounters(uint64_t* enq, uint64_t* deq);

// Poll for a result. Returns 1 if a result was retrieved, 0 if queue is empty.
int PollResult(CSeckillResult* res);

#ifdef __cplusplus
}
#endif

#endif // BRIDGE_H
