package metrics

import (
	"sync/atomic"
)

// Global atomic counters for Phase A testing
var (
	RequestsTotal       uint64
	EnqueueOKTotal      uint64
	EnqueueRejectTotal  uint64
	EnqueueLatencyTotal uint64 // Total nanoseconds
	SoldTotal           uint64 // Total items sold

	// Observability Metrics
	ResultQueueDepth    uint64
	DBBatchSizeLast     uint64
	DBFlushIntervalLast uint64 // ms
	DBCommitLatencyLast uint64 // ms

	// Queue Latency Metrics (Aggregate)
	MpmcLatencyTotal uint64 // Total nanoseconds
	MpmcCount        uint64
	SpscLatencyTotal uint64 // Total nanoseconds
	SpscCount        uint64
)

func AddRequest() {
	atomic.AddUint64(&RequestsTotal, 1)
}

func AddEnqueueOK() {
	atomic.AddUint64(&EnqueueOKTotal, 1)
}

func AddEnqueueReject() {
	atomic.AddUint64(&EnqueueRejectTotal, 1)
}

func AddSold(qty int) {
	atomic.AddUint64(&SoldTotal, uint64(qty))
}

func AddLatency(ns int64) {
	atomic.AddUint64(&EnqueueLatencyTotal, uint64(ns))
}

func AddQueueLatency(mpmcNs, spscNs int64) {
	if mpmcNs >= 0 {
		atomic.AddUint64(&MpmcLatencyTotal, uint64(mpmcNs))
		atomic.AddUint64(&MpmcCount, 1)
	}
	if spscNs >= 0 {
		atomic.AddUint64(&SpscLatencyTotal, uint64(spscNs))
		atomic.AddUint64(&SpscCount, 1)
	}
}

func SetResultQueueDepth(val uint64) { atomic.StoreUint64(&ResultQueueDepth, val) }
func SetDBBatchSize(val uint64)      { atomic.StoreUint64(&DBBatchSizeLast, val) }
func SetDBFlushInterval(val uint64)  { atomic.StoreUint64(&DBFlushIntervalLast, val) }
func SetDBCommitLatency(val uint64)  { atomic.StoreUint64(&DBCommitLatencyLast, val) }

func GetStats() map[string]uint64 {
	return map[string]uint64{
		"requests_total":         atomic.LoadUint64(&RequestsTotal),
		"enqueue_ok_total":       atomic.LoadUint64(&EnqueueOKTotal),
		"enqueue_reject_total":   atomic.LoadUint64(&EnqueueRejectTotal),
		"enqueue_latency_total":  atomic.LoadUint64(&EnqueueLatencyTotal),
		"sold_total":             atomic.LoadUint64(&SoldTotal),
		"result_queue_depth":     atomic.LoadUint64(&ResultQueueDepth),
		"db_batch_size_last":     atomic.LoadUint64(&DBBatchSizeLast),
		"db_flush_interval_last": atomic.LoadUint64(&DBFlushIntervalLast),
		"db_commit_latency_last": atomic.LoadUint64(&DBCommitLatencyLast),
		"mpmc_latency_total":     atomic.LoadUint64(&MpmcLatencyTotal),
		"mpmc_count":             atomic.LoadUint64(&MpmcCount),
		"spsc_latency_total":     atomic.LoadUint64(&SpscLatencyTotal),
		"spsc_count":             atomic.LoadUint64(&SpscCount),
	}
}
