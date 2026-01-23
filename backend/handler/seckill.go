package handler

/*
#cgo CXXFLAGS: -std=c++20
#cgo LDFLAGS: -static-libstdc++ -static-libgcc
#include "bridge.h"
*/
import "C"

import (
	"fmt"
	"math/rand"
	"net/http"
	"seckillapp/cache"
	"seckillapp/db"
	"seckillapp/metrics"
	"seckillapp/model"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm/clause"
)

var (
	// Circuit Breaker for Redis
	redisFailureCount atomic.Uint32
	redisLastFailure  atomic.Int64
)

const (
	RedisFailureThreshold = 5
	RedisCircuitDuration  = 5 // seconds
)

type SeckillRequest struct {
	SkuID     int64  `json:"sku_id"`
	Qty       int    `json:"qty"`
	GuestID   uint64 `json:"-"` // Parsed from Header
	RequestID uint64 `json:"-"` // Parsed from Header
}

var (
	shutdownChan = make(chan struct{})
	dbWorkerDone = make(chan struct{})
	orderChan    chan model.Order

	// Atomic Counters for Closed-Loop Shutdown
	AcceptedRequests uint64
	ResultsReceived  uint64
	ResultsSoldOut   uint64
	DBCommitted      uint64

	// DB Metrics
	DBBatchCount   uint64
	DBTotalLatency uint64 // Microseconds

	// DB Latency Distribution (Protected by Mutex)
	dbMetricsMu  sync.Mutex
	dbLatencies  []int64 // Microseconds
	dbBatchSizes []int

	// Cleanup Worker Metrics
	CleanupBatchCount uint64
	CleanupTotalItems uint64
)

type CleanupTask struct {
	SkuID     int64
	GuestID   uint64
	Qty       int
	RequestID uint64
	Status    int // 1=Success (RemovePending), 2=SoldOut (Rollback)
}

var cleanupChan chan CleanupTask

// StartConsumer initializes the C++ engine.
func StartConsumer() {
	C.InitEngine()
	// Reset channels
	shutdownChan = make(chan struct{})
	dbWorkerDone = make(chan struct{})
	// Order Processing Channel
	orderChan = make(chan model.Order, 10000)
	cleanupChan = make(chan CleanupTask, 10000)

	// Shutdown Signals
	go dbWorker()
	go cleanupWorker()

	// Launch Parallel Result Processors
	for i := 0; i < 8; i++ {
		go processResultWorker(i)
	}
	go StartSweeper()
}

// WarmUpSystem performs full system pre-heating
func WarmUpSystem() {
	fmt.Println("=== System WarmUp Started ===")

	// 1. Redis Warmup
	if cache.Rdb != nil {
		fmt.Print("Redis: Pinging... ")
		for i := 0; i < 10; i++ {
			cache.Rdb.Ping(cache.Ctx)
		}
		fmt.Println("Done.")
	}

	// 2. DB Warmup
	if db.DB != nil {
		fmt.Print("DB: Pinging... ")
		sqlDB, _ := db.DB.DB()
		if sqlDB != nil {
			for i := 0; i < 10; i++ {
				sqlDB.Ping()
			}
		}
		fmt.Println("Done.")
	}

	// 3. C++ Engine Warmup
	fmt.Println("C++ Engine: Injecting WarmUp Traffic...")
	C.WarmUpEngine()
	fmt.Println("=== System WarmUp Completed ===")
}

func processResultWorker(id int) {
	var res C.CSeckillResult
	fmt.Printf("Async Consumer %d: Polling Started\n", id)

loop:
	for {
		select {
		case <-shutdownChan:
			fmt.Printf("Async Consumer %d: Shutdown signal received\n", id)
			break loop
		default:
			if C.PollResult(&res) != 0 {
				// Convert C struct to Go Model
				status := int(res.status)

				// Always increment ResultsReceived for closed-loop shutdown
				val := atomic.AddUint64(&ResultsReceived, 1)

				if val <= 0 { // Disabled Debug Log
					fmt.Printf("DEBUG: Status=%d, MPMC=%d, SPSC=%d, Ingress=%d, PopMPMC=%d\n",
						status, res.mpmc_latency_ns, res.spsc_latency_ns, res.ts_ingress, res.ts_pop_mpmc)
				}

				if status == 1 {
					// Collect Queue Latency Metrics
					metrics.AddQueueLatency(int64(res.mpmc_latency_ns), int64(res.spsc_latency_ns))

					// 0. Benchmark Pure Queue Mode
					// If RequestID is very large, it's a pure queue benchmark.
					// Skip DB and Redis persistence.
					if uint64(res.request_id) > 18000000000000000000 {
						continue
					}

					order := model.Order{
						ID:      uint64(res.request_id),
						SkuID:   int64(res.sku_id),
						GuestID: uint64(res.guest_id),
						Qty:     int32(res.qty),
						Status:  1, // Created
					}
					// Push to Memory Buffer (Will block if full -> Backpressure)
					metrics.SetResultQueueDepth(uint64(len(orderChan)))

					// 1. Persist (OrderChan) - DB Worker handles idempotency
					select {
					case orderChan <- order:
					case <-shutdownChan:
						fmt.Printf("Async Consumer %d: Shutdown signal received while pushing\n", id)
						orderChan <- order
						break loop
					}

					// 2. Cleanup (Redis) - Async
					cleanupChan <- CleanupTask{
						SkuID:     int64(res.sku_id),
						GuestID:   uint64(res.guest_id),
						Qty:       int(res.qty),
						RequestID: uint64(res.request_id),
						Status:    1,
					}

				} else if status == 2 {
					atomic.AddUint64(&ResultsSoldOut, 1)
					// C++ Engine found it sold out (Double Check)
					// Push to Cleanup for Rollback
					cleanupChan <- CleanupTask{
						SkuID:     int64(res.sku_id),
						GuestID:   uint64(res.guest_id),
						Qty:       int(res.qty),
						RequestID: uint64(res.request_id),
						Status:    2,
					}
				} else {
					// status == 0 or other errors
					// Internal Engine Failure
					// Also Rollback
					cleanupChan <- CleanupTask{
						SkuID:     int64(res.sku_id),
						GuestID:   uint64(res.guest_id),
						Qty:       int(res.qty),
						RequestID: uint64(res.request_id),
						Status:    status,
					}
					fmt.Printf("Rolled back Req %d (Status %d: Internal Failure)\n", res.request_id, status)
				}
			} else {
				// Sleep briefly to avoid busy loop if queue is empty
				time.Sleep(1 * time.Millisecond)
			}
		}
	}
	// Worker exits
}

// processResults removed (replaced by processResultWorker)

func cleanupWorker() {
	const batchSize = 100 // Larger batch for Redis
	const flushInterval = 5 * time.Millisecond
	var batch []CleanupTask = make([]CleanupTask, 0, batchSize)

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	fmt.Println("Async Cleanup Worker Started")

	flush := func() {
		if len(batch) > 0 {
			if cache.Rdb == nil {
				fmt.Println("Error: Redis client is nil in Cleanup Worker")
				batch = batch[:0]
				return
			}

			pipe := cache.Rdb.Pipeline()

			for _, task := range batch {
				if task.Status == 1 {
					// Success: Remove Pending + Mark Done
					member := fmt.Sprintf("%d:%d:%d:%d", task.SkuID, task.GuestID, task.Qty, task.RequestID)
					pendingKey := fmt.Sprintf("seckill:{%d}:pending", task.SkuID)
					doneKey := fmt.Sprintf("done:{%d}", task.RequestID)

					// 1. Mark Done (Idempotency Key for 60s)
					pipe.SetNX(cache.Ctx, doneKey, 1, 60*time.Second)

					// 2. Remove from Pending
					pipe.ZRem(cache.Ctx, pendingKey, member)
				} else {
					// Rollback (Status 2 or others)
					// For simplicity, we just trigger RollbackInventory goroutine or do it here?
					// RollbackInventory involves Lua script. Can we pipeline Lua? Yes.
					// But RollbackInventory function logic is complex (retries).
					// For now, let's keep it simple: spawn goroutine for Rollback (since it's error path)
					// Or call it directly if we want to block cleanup worker? No.
					go func(t CleanupTask) {
						cache.RollbackInventory(t.SkuID, t.GuestID, t.Qty, t.RequestID)
					}(task)
				}
			}

			_, err := pipe.Exec(cache.Ctx)
			if err != nil {
				fmt.Println("Redis Cleanup Pipeline Error:", err)
			}

			atomic.AddUint64(&CleanupBatchCount, 1)
			atomic.AddUint64(&CleanupTotalItems, uint64(len(batch)))

			batch = batch[:0]
		}
	}

	for {
		select {
		case task := <-cleanupChan:
			batch = append(batch, task)
			if len(batch) >= batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func dbWorker() {
	const batchSize = 1000
	const flushInterval = 10 * time.Millisecond // Reduced from 200ms for lower latency
	var batch []model.Order = make([]model.Order, 0, batchSize)

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	defer close(dbWorkerDone)

	fmt.Println("Async DB Worker Started")

	flush := func() {
		if len(batch) > 0 {
			if db.DB == nil {
				// DB disabled (e.g. benchmark)
				batch = batch[:0]
				return
			}
			start := time.Now()
			db.DB.Clauses(clause.OnConflict{DoNothing: true}).Create(&batch)
			latency := time.Since(start).Microseconds()

			atomic.AddUint64(&DBCommitted, uint64(len(batch)))
			atomic.AddUint64(&DBBatchCount, 1)
			atomic.AddUint64(&DBTotalLatency, uint64(latency))

			// Collect Distribution Metrics
			dbMetricsMu.Lock()
			dbLatencies = append(dbLatencies, latency)
			dbBatchSizes = append(dbBatchSizes, len(batch))
			// Cap size to avoid infinite growth in long runs (optional but safe)
			if len(dbLatencies) > 20000 {
				dbLatencies = dbLatencies[1:]
				dbBatchSizes = dbBatchSizes[1:]
			}
			dbMetricsMu.Unlock()

			batch = batch[:0]
		}
	}

	for {
		select {
		case order, ok := <-orderChan:
			if !ok {
				// Channel closed, flush remaining
				flush()
				fmt.Println("Async DB Worker Stopped")
				return
			}
			batch = append(batch, order)
			if len(batch) >= batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func StopConsumer() {
	// 5. Stop Engine (Join threads)
	fmt.Println("Stopping C++ Engine...")
	C.StopEngine()
	fmt.Println("Shutdown Complete.")
}

func EnableCorrectnessCheck(enabled bool) {
	if enabled {
		C.EnableCorrectnessCheck(1)
	} else {
		C.EnableCorrectnessCheck(0)
	}
}

func GetConsumerStats() (lastSeq uint64, gapCount uint64, dupCount uint64) {
	var cLastSeq C.uint64_t
	var cGapCount C.uint64_t
	var cDupCount C.uint64_t
	C.GetConsumerStats(&cLastSeq, &cGapCount, &cDupCount)
	return uint64(cLastSeq), uint64(cGapCount), uint64(cDupCount)
}

func EnqueueBatchRaw(ptr unsafe.Pointer, count int) int {
	return int(C.EnqueueBatch((*C.CSeckillRequest)(ptr), C.int(count)))
}

func EnqueueWithSeq(seq uint64) {
	var cReq C.CSeckillRequest
	cReq.sku_id = 123
	cReq.qty = 1
	cReq.guest_id = 1001
	cReq.request_id = C.uint64_t(seq)
	C.EnqueueRequest(cReq)
}

func EnqueueHandler(c *gin.Context) {
	// start := time.Now()
	metrics.AddRequest()

	// 1. Parse Headers
	guestIDStr := c.GetHeader("X-Guest-Id")
	reqIDStr := c.GetHeader("X-Request-Id")

	var guestID uint64
	var reqID uint64
	var err error

	if guestIDStr != "" {
		guestID, _ = strconv.ParseUint(guestIDStr, 10, 64)
	} else {
		// Fallback for easy testing: random guest
		// Use Int63 to avoid SQLite uint64 high-bit issue
		guestID = uint64(rand.Int63())
	}

	if reqIDStr != "" {
		reqID, _ = strconv.ParseUint(reqIDStr, 10, 64)
	} else {
		println("Missing X-Request-Id header. Headers:", c.Request.Header)
		for k, v := range c.Request.Header {
			println(k, ":", v[0])
		}
		// Fallback: random reqID
		reqID = uint64(rand.Int63())
	}

	// 2. Parse Body
	var req SeckillRequest
	if err = c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON"})
		return
	}
	req.GuestID = guestID
	req.RequestID = reqID

	// 2.5 Redis Pre-check (Distributed Gatekeeper)
	if cache.Rdb != nil {
		// Circuit Breaker Check
		if redisFailureCount.Load() > RedisFailureThreshold {
			if time.Now().Unix()-redisLastFailure.Load() < RedisCircuitDuration {
				// Circuit Open: Fail Fast
				c.JSON(http.StatusServiceUnavailable, gin.H{"error": "System Busy (Storage Layer)"})
				return
			}
			// Half-open: Allow one request through (or all, relying on atomic reset)
		}

		status, err := cache.DeductInventory(req.SkuID, req.GuestID, req.Qty, req.RequestID)
		if err != nil {
			// Record Failure
			redisFailureCount.Add(1)
			redisLastFailure.Store(time.Now().Unix())

			fmt.Printf("Redis Error: %v\n", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "System Error"})
			return
		}
		// Success: Reset Failure Count
		redisFailureCount.Store(0)

		if status == 0 { // Sold Out
			metrics.AddEnqueueReject()
			c.JSON(http.StatusConflict, gin.H{"error": "Sold out"})
			return
		} else if status == -1 { // Duplicate
			c.JSON(http.StatusConflict, gin.H{"error": "Duplicate purchase"})
			return
		} else if status == -2 { // Stock Not Initialized
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid SKU or System Not Ready"})
			return
		}
		// status == 1: Success -> Proceed to C++ Engine
		// Pending Log is now handled atomically inside DeductInventory (Lua)

		// Register SKU for Sweeper (Best Effort)
		cache.Rdb.SAdd(cache.Ctx, "seckill:skus", req.SkuID)
	}

	// 3. Enqueue to C++ Engine
	var cReq C.CSeckillRequest
	cReq.sku_id = C.int64_t(req.SkuID)
	cReq.qty = C.int(req.Qty)
	cReq.guest_id = C.uint64_t(req.GuestID)
	cReq.request_id = C.uint64_t(req.RequestID)

	// Pass to C++ Ring Buffer
	res := C.EnqueueRequest(cReq)
	if res == 1 {
		atomic.AddUint64(&AcceptedRequests, 1)
		c.JSON(http.StatusAccepted, gin.H{"status": "queued", "request_id": reqID})
	} else if res == 2 {
		// Sold Out (Fast Fail)
		metrics.AddEnqueueReject()
		if cache.Rdb != nil {
			cache.RollbackInventory(req.SkuID, req.GuestID, req.Qty, req.RequestID)
		}
		c.JSON(http.StatusConflict, gin.H{"error": "Sold out"})
	} else {
		// Queue Full - Rollback Redis!
		metrics.AddEnqueueReject()
		if cache.Rdb != nil {
			cache.RollbackInventory(req.SkuID, req.GuestID, req.Qty, req.RequestID)
			// RemovePendingRequest is handled inside RollbackInventory Lua script
		}
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Queue full"})
	}

	// elapsed := time.Since(start)
	// metrics.RecordLatency(elapsed.Seconds())
}

// StatsHandler returns current metrics
func StatsHandler(c *gin.Context) {
	var enq, deq C.uint64_t
	C.GetQueueCounters(&enq, &deq)

	dbCount := atomic.LoadUint64(&DBBatchCount)
	dbLat := atomic.LoadUint64(&DBTotalLatency)
	var avgDBLat float64 = 0
	if dbCount > 0 {
		avgDBLat = float64(dbLat) / float64(dbCount)
	}

	cleanupCount := atomic.LoadUint64(&CleanupBatchCount)
	cleanupItems := atomic.LoadUint64(&CleanupTotalItems)
	var avgCleanupBatch float64 = 0
	if cleanupCount > 0 {
		avgCleanupBatch = float64(cleanupItems) / float64(cleanupCount)
	}

	// Calculate Percentiles
	dbMetricsMu.Lock()
	latencies := make([]int64, len(dbLatencies))
	copy(latencies, dbLatencies)
	sizes := make([]int, len(dbBatchSizes))
	copy(sizes, dbBatchSizes)
	dbMetricsMu.Unlock()

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	sort.Ints(sizes)

	getPercentile := func(data []int64, p float64) int64 {
		if len(data) == 0 {
			return 0
		}
		idx := int(float64(len(data)-1) * p)
		return data[idx]
	}

	getPercentileInt := func(data []int, p float64) int {
		if len(data) == 0 {
			return 0
		}
		idx := int(float64(len(data)-1) * p)
		return data[idx]
	}

	c.JSON(http.StatusOK, gin.H{
		"accepted_requests":   atomic.LoadUint64(&AcceptedRequests),
		"results_received":    atomic.LoadUint64(&ResultsReceived),
		"results_sold_out":    atomic.LoadUint64(&ResultsSoldOut),
		"db_committed":        atomic.LoadUint64(&DBCommitted),
		"queue_depth":         len(orderChan),
		"cpp_enqueue":         uint64(enq),
		"cpp_dequeue":         uint64(deq),
		"db_batch_count":      dbCount,
		"db_avg_latency_us":   avgDBLat,
		"db_latency_p50":      getPercentile(latencies, 0.50),
		"db_latency_p90":      getPercentile(latencies, 0.90),
		"db_latency_p99":      getPercentile(latencies, 0.99),
		"db_latency_max":      getPercentile(latencies, 1.00),
		"db_batch_size_p50":   getPercentileInt(sizes, 0.50),
		"db_batch_size_p90":   getPercentileInt(sizes, 0.90),
		"db_batch_size_p99":   getPercentileInt(sizes, 0.99),
		"db_batch_size_max":   getPercentileInt(sizes, 1.00),
		"cleanup_batch_count": cleanupCount,
		"cleanup_avg_size":    avgCleanupBatch,
		"mpmc_count":          atomic.LoadUint64(&metrics.MpmcCount),
		"mpmc_latency_total":  atomic.LoadUint64(&metrics.MpmcLatencyTotal),
		"spsc_count":          atomic.LoadUint64(&metrics.SpscCount),
		"spsc_latency_total":  atomic.LoadUint64(&metrics.SpscLatencyTotal),
	})
}

// CountHandler checks actual DB row count
func CountHandler(c *gin.Context) {
	var count int64
	if db.DB != nil {
		db.DB.Model(&model.Order{}).Count(&count)
	}
	c.JSON(http.StatusOK, gin.H{"count": count})
}

// PrintQueueCounters fetches and prints internal C++ queue counters.
func PrintQueueCounters() {
	var enq, deq C.uint64_t
	C.GetQueueCounters(&enq, &deq)
	fmt.Printf("\n=== C++ Queue Counters ===\n")
	fmt.Printf("Enqueue (Success): %d\n", uint64(enq))
	fmt.Printf("Dequeue (Dispatcher): %d\n", uint64(deq))
	fmt.Printf("==========================\n")
}

// StartSweeper periodically checks for stale pending requests and rolls them back.
func StartSweeper() {
	fmt.Println("Sweeper: Started. Checking for stale requests every 5 seconds.")
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		// 1. Get Active SKUs
		skus, err := cache.Rdb.SMembers(cache.Ctx, "seckill:skus").Result()
		if err != nil {
			// fmt.Printf("Sweeper: Error getting active SKUs: %v\n", err) // Benign if set is empty or redis transient
			continue
		}

		for _, skuStr := range skus {
			skuID, _ := strconv.ParseInt(skuStr, 10, 64)

			// Timeout: 30 seconds. If a request is pending for > 30s, assume crash/loss.
			staleMembers, err := cache.GetStalePendingRequests(skuID, 30)
			if err != nil {
				// fmt.Printf("Sweeper: Error getting stale requests for SKU %d: %v\n", skuID, err)
				continue
			}

			for _, member := range staleMembers {
				// Format: "sku:user:qty:reqID"
				var sku int64
				var user, reqID uint64
				var qty int
				n, err := fmt.Sscanf(member, "%d:%d:%d:%d", &sku, &user, &qty, &reqID)
				if err != nil || n != 4 {
					fmt.Printf("Sweeper: Invalid member format '%s': %v. Removing.\n", member, err)
					// Manually remove invalid member from its ZSET
					cache.RemovePendingRequest(skuID, 0, 0, 0) // HACK: RemovePendingRequest constructs key from SKU. But member is needed.
					// We need a raw ZRem helper or just fix RemovePendingRequest to take raw member?
					// Let's just ignore for now or use Rdb directly.
					cache.Rdb.ZRem(cache.Ctx, fmt.Sprintf("seckill:{%d}:pending", skuID), member)
					continue
				}

				fmt.Printf("Sweeper: Found stale request %d (User %d, SKU %d). Rolling back...\n", reqID, user, sku)

				// Rollback Inventory (handles ZREM internally)
				err = cache.RollbackInventory(sku, user, qty, reqID)
				if err != nil {
					fmt.Printf("Sweeper: Failed to rollback request %d: %v\n", reqID, err)
				} else {
					fmt.Printf("Sweeper: Rolled back and removed request %d\n", reqID)
				}
			}
		}
	}
}
