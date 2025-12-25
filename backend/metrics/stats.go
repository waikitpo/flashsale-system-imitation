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

func GetStats() map[string]uint64 {
	return map[string]uint64{
		"requests_total":        atomic.LoadUint64(&RequestsTotal),
		"enqueue_ok_total":      atomic.LoadUint64(&EnqueueOKTotal),
		"enqueue_reject_total":  atomic.LoadUint64(&EnqueueRejectTotal),
		"enqueue_latency_total": atomic.LoadUint64(&EnqueueLatencyTotal),
		"sold_total":            atomic.LoadUint64(&SoldTotal),
	}
}
