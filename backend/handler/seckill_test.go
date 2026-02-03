package handler_test

import (
	"os"
	"seckillapp/cache"
	"seckillapp/config"
	"seckillapp/db"
	"seckillapp/handler"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

func init() {
	// Fix CWD for tests to find db/ and config/
	// os.Chdir("..")

	config.InitConfig()
	os.Setenv("USE_PG", "false") // Force SQLite
	db.InitDB()
	cache.InitRedis("127.0.0.1:6380", "", 0)

	// Initialize C++ engine once for all benchmarks
	handler.StartConsumer()
	// Give it a moment to start
	time.Sleep(100 * time.Millisecond)
}

// Single-threaded SPSC Benchmark
func BenchmarkSPSCEnqueue(b *testing.B) {
	// handler.EnableCorrectnessCheck(false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.EnqueueWithSeq(uint64(i))
	}
}

// Batch Benchmark
func BenchmarkBatchEnqueue(b *testing.B) {
	// handler.EnableCorrectnessCheck(false)

	// Prepare batch buffer
	batchSize := 128

	// Match C struct layout: int64, int32, padding(4), uint64, uint64, 4*int64
	type CRequest struct {
		SkuID      int64
		Qty        int32
		_          int32 // padding
		GuestID    uint64
		RequestID  uint64
		TsIngress  int64
		TsPopMpmc  int64
		TsPushSpsc int64
		TsPopSpsc  int64
	}

	reqs := make([]CRequest, batchSize)
	// Round-robin SKUs: 1001-1008
	skuIDs := []int64{1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008}
	baseReqID := uint64(18000000000000000001)

	for i := 0; i < batchSize; i++ {
		reqs[i].SkuID = skuIDs[i%len(skuIDs)]
		reqs[i].Qty = 1
		reqs[i].GuestID = 1001
		reqs[i].RequestID = baseReqID + uint64(i)
	}

	ptr := unsafe.Pointer(&reqs[0])
	b.ResetTimer()

	totalEnqueued := 0
	pureEnqueueTime := int64(0)
	for i := 0; i < b.N; i++ {
		start := time.Now()
		// EnqueueBatchRaw returns the number of actually enqueued items
		n := handler.EnqueueBatchRaw(ptr, batchSize)
		duration := time.Since(start).Nanoseconds()

		totalEnqueued += n

		if n > 0 {
			pureEnqueueTime += duration
		}

		// Simple backpressure simulation
		if n < batchSize {
			// runtime.Gosched()
		}
	}

	b.StopTimer()

	if totalEnqueued > 0 {
		// Report backpressure-aware throughput
		b.ReportMetric(float64(totalEnqueued)/b.Elapsed().Seconds(), "msgs/sec_backpressure")

		// Report pure enqueue cost
		if pureEnqueueTime > 0 {
			nsPerMsgPure := float64(pureEnqueueTime) / float64(totalEnqueued)
			b.ReportMetric(nsPerMsgPure, "ns/msg_pure")
			b.ReportMetric(1e9/nsPerMsgPure, "msgs/sec_pure")
		}
	}
}

// Hotspot Benchmark: All requests to SKU 123 (Worker X)
func BenchmarkHotspot(b *testing.B) {
	// Prepare batch buffer
	batchSize := 128
	type CRequest struct {
		SkuID      int64
		Qty        int32
		_          int32 // padding
		GuestID    uint64
		RequestID  uint64
		TsIngress  int64
		TsPopMpmc  int64
		TsPushSpsc int64
		TsPopSpsc  int64
	}

	reqs := make([]CRequest, batchSize)
	for i := 0; i < batchSize; i++ {
		reqs[i].SkuID = 123
		reqs[i].Qty = 1
		reqs[i].GuestID = 1001
		reqs[i].RequestID = uint64(i)
	}

	ptr := unsafe.Pointer(&reqs[0])
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		handler.EnqueueBatchRaw(ptr, batchSize)
	}
}

// Balanced Benchmark: Requests distributed across SKUs
func BenchmarkBalanced(b *testing.B) {
	// Prepare batch buffer
	batchSize := 128
	type CRequest struct {
		SkuID      int64
		Qty        int32
		_          int32 // padding
		GuestID    uint64
		RequestID  uint64
		TsIngress  int64
		TsPopMpmc  int64
		TsPushSpsc int64
		TsPopSpsc  int64
	}

	reqs := make([]CRequest, batchSize)
	for i := 0; i < batchSize; i++ {
		reqs[i].SkuID = 123
		reqs[i].Qty = 1
		reqs[i].GuestID = 1001
		reqs[i].RequestID = uint64(i)
	}

	ptr := unsafe.Pointer(&reqs[0])
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		handler.EnqueueBatchRaw(ptr, batchSize)
	}
}

// HOL Blocking Benchmark: 90% Hot (123), 10% Cold (456)
func BenchmarkHolBlocking(b *testing.B) {
	batchSize := 128
	type CRequest struct {
		SkuID      int64
		Qty        int32
		_          int32
		GuestID    uint64
		RequestID  uint64
		TsIngress  int64
		TsPopMpmc  int64
		TsPushSpsc int64
		TsPopSpsc  int64
	}

	hotBatch := make([]CRequest, batchSize)
	for i := range hotBatch {
		hotBatch[i].SkuID = 123 // Hot
		hotBatch[i].Qty = 1
		hotBatch[i].GuestID = 1001
		hotBatch[i].RequestID = uint64(i)
	}

	coldBatch := make([]CRequest, batchSize)
	for i := range coldBatch {
		coldBatch[i].SkuID = 456 // Cold
		coldBatch[i].Qty = 1
		coldBatch[i].GuestID = 2002
		coldBatch[i].RequestID = uint64(i)
	}

	hotPtr := unsafe.Pointer(&hotBatch[0])
	coldPtr := unsafe.Pointer(&coldBatch[0])

	b.ResetTimer()
	b.SetParallelism(10) // 10 threads to ensure mixing

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			// 90% Hot, 10% Cold
			if i%10 == 0 {
				handler.EnqueueBatchRaw(coldPtr, batchSize)
			} else {
				handler.EnqueueBatchRaw(hotPtr, batchSize)
			}
			i++
		}
	})
}

func TestCorrectness(t *testing.T) {
	handler.EnableCorrectnessCheck(true)
	// Give consumer time to reset
	time.Sleep(100 * time.Millisecond)

	count := 100000

	// Enqueue 1..N
	for i := 1; i <= count; i++ {
		handler.EnqueueWithSeq(uint64(i))
	}

	// Wait for drain (consumer is polling)
	// With 100k items, it should be very fast (<10ms), but sleep longer to be safe
	time.Sleep(500 * time.Millisecond)

	lastSeq, gap, dup := handler.GetConsumerStats()

	t.Logf("Correctness Check: Sent=%d, LastSeq=%d, Gap=%d, Dup=%d", count, lastSeq, gap, dup)

	if lastSeq != uint64(count) {
		t.Errorf("Expected last seq %d, got %d. (Maybe queue full dropped some?)", count, lastSeq)
	}
	if gap != 0 {
		t.Errorf("Gap count %d, expected 0. (Dropped items?)", gap)
	}
	if dup != 0 {
		t.Errorf("Dup count %d, expected 0. (Duplicate processing?)", dup)
	}
}

func BenchmarkPureQueueOverhead(b *testing.B) {
	// Give consumer time to reset/drain from previous tests
	time.Sleep(500 * time.Millisecond)

	batchSize := 128
	type CRequest struct {
		SkuID      int64
		Qty        int32
		_          int32
		GuestID    uint64
		RequestID  uint64
		TsIngress  int64
		TsPopMpmc  int64
		TsPushSpsc int64
		TsPopSpsc  int64
	}

	reqs := make([]CRequest, batchSize)
	// Use special RequestID to trigger Pure Queue Mode in Worker
	// 18446744073709551615 is UINT64_MAX, used for barrier.
	// Let's use 18446744073709551000 (just below max) as base
	baseReqID := uint64(18000000000000000001)

	// Round-robin SKUs: 1001-1008
	skuIDs := []int64{1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008}

	for i := 0; i < batchSize; i++ {
		reqs[i].SkuID = skuIDs[i%len(skuIDs)]
		reqs[i].Qty = 1
		reqs[i].GuestID = 1001
		reqs[i].RequestID = baseReqID + uint64(i)
	}

	ptr := unsafe.Pointer(&reqs[0])

	b.ResetTimer()
	b.SetBytes(int64(batchSize * 64)) // Approx size of struct

	totalAccepted := int64(0)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := handler.EnqueueBatchRaw(ptr, batchSize)
			atomic.AddInt64(&totalAccepted, int64(n))
		}
	})

	b.StopTimer()
	elapsed := b.Elapsed().Seconds()
	ops := float64(totalAccepted) / elapsed
	b.ReportMetric(ops, "msgs/sec_pure_queue")
	b.ReportMetric(1e9/ops, "ns/msg_pure_queue")
}

func TestFailFast(t *testing.T) {
	// Give consumer time to reset/drain from previous tests
	time.Sleep(500 * time.Millisecond)

	batchSize := 128
	type CRequest struct {
		SkuID      int64
		Qty        int32
		_          int32
		GuestID    uint64
		RequestID  uint64
		TsIngress  int64
		TsPopMpmc  int64
		TsPushSpsc int64
		TsPopSpsc  int64
	}

	// Prepare Hot Batch (SKU 123)
	hotBatch := make([]CRequest, batchSize)
	for i := range hotBatch {
		hotBatch[i].SkuID = 123
		hotBatch[i].Qty = 1
		hotBatch[i].GuestID = 1001
		hotBatch[i].RequestID = uint64(i)
	}
	hotPtr := unsafe.Pointer(&hotBatch[0])

	// Prepare Cold Batch (SKU 456)
	coldBatch := make([]CRequest, batchSize)
	for i := range coldBatch {
		coldBatch[i].SkuID = 456
		coldBatch[i].Qty = 1
		coldBatch[i].GuestID = 2002
		coldBatch[i].RequestID = uint64(i)
	}
	coldPtr := unsafe.Pointer(&coldBatch[0])

	// Flood Hot Shard
	totalSent := 0
	rejectedCount := 0
	acceptedCount := 0

	// Queue size is 4096. Sending 100k items should definitely fill it if processing is slower than ingestion.
	// Since we are in Go, calling C function, ingestion is very fast.
	for i := 0; i < 1000; i++ { // 1000 * 128 = 128,000 items
		n := handler.EnqueueBatchRaw(hotPtr, batchSize)
		acceptedCount += n
		rejectedCount += (batchSize - n)
		totalSent += batchSize
	}

	t.Logf("Hot Shard (123): Sent=%d, Accepted=%d, Rejected=%d", totalSent, acceptedCount, rejectedCount)

	if rejectedCount == 0 {
		t.Errorf("Expected rejections (Fail-Fast) but got none. Queue draining too fast or test logic flaw?")
	}

	// Verify Cold Shard (456) is still accepting
	// Note: If Hot Shard blocked the whole system (HOL), Cold Shard would also be rejected or blocked.
	// But we have multi-queue, so Cold Shard should be fine.
	nCold := handler.EnqueueBatchRaw(coldPtr, batchSize)
	if nCold != batchSize {
		t.Errorf("Cold Shard (456) rejected requests! Isolation failure? Accepted %d/%d", nCold, batchSize)
	} else {
		t.Logf("Cold Shard (456) accepted all %d requests (Isolation OK)", batchSize)
	}
}
