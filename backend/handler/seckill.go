package handler

/*
#cgo CXXFLAGS: -std=c++20 -I${SRCDIR}/../../engine/src
#include "../../engine/src/bridge.h"
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
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm/clause"
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
)

// StartConsumer initializes the C++ engine.
func StartConsumer() {
	C.InitEngine()
	// Reset channels
	shutdownChan = make(chan struct{})
	dbWorkerDone = make(chan struct{})
	// Order Processing Channel
	orderChan = make(chan model.Order, 10000)

	// Shutdown Signals
	go dbWorker()
	go processResults()
	go StartSweeper()
}

func processResults() {
	var res C.CSeckillResult
	fmt.Println("Async Consumer: Polling Started")

loop:
	for {
		select {
		case <-shutdownChan:
			fmt.Println("Async Consumer: Shutdown signal received")
			break loop
		default:
			if C.PollResult(&res) != 0 {
				// Convert C struct to Go Model
				status := int(res.status)

				// Always increment ResultsReceived for closed-loop shutdown
				atomic.AddUint64(&ResultsReceived, 1)

				if status == 1 {
					// Remove from Pending (In-Flight)
					cache.RemovePendingRequest(int64(res.sku_id), uint64(res.guest_id), int(res.qty), uint64(res.request_id))

					order := model.Order{
						ID:      uint64(res.request_id),
						SkuID:   int64(res.sku_id),
						GuestID: uint64(res.guest_id),
						Qty:     int32(res.qty),
						Status:  1, // Created
					}
					// Push to Memory Buffer (Will block if full -> Backpressure)
					metrics.SetResultQueueDepth(uint64(len(orderChan)))
					select {
					case orderChan <- order:
					case <-shutdownChan:
						fmt.Println("Async Consumer: Shutdown signal received while pushing")
						orderChan <- order
						break loop
					}
				} else if status == 2 {
					atomic.AddUint64(&ResultsSoldOut, 1)
					// C++ Engine found it sold out (Double Check)
					// We must rollback Redis because we already decremented it.
					if cache.Rdb != nil {
						go func(sku int64, guest uint64, q int, rID uint64) {
							cache.RollbackInventory(sku, guest, q, rID)
							// RemovePendingRequest is handled inside RollbackInventory
							fmt.Printf("Rolled back Req %d (Status 2: SoldOut from Engine)\n", rID)
						}(int64(res.sku_id), uint64(res.guest_id), int(res.qty), uint64(res.request_id))
					}
				} else {
					// status == 0 or other errors
					// Internal Engine Failure (e.g. WAL error)
					if cache.Rdb != nil {
						go func(sku int64, guest uint64, q int, rID uint64) {
							// fmt.Printf("DEBUG: Simulating Crash... Sleeping 60s before rollback for Req %d\n", rID)
							// time.Sleep(60 * time.Second)
							cache.RollbackInventory(sku, guest, q, rID)
							// RemovePendingRequest is handled inside RollbackInventory
							fmt.Printf("Rolled back Req %d (Status %d: Internal Failure)\n", rID, status)
						}(int64(res.sku_id), uint64(res.guest_id), int(res.qty), uint64(res.request_id))
					}
				}
			} else {
				// Sleep briefly to avoid busy loop if queue is empty
				time.Sleep(1 * time.Millisecond)
			}
		}
	}

	// Draining Phase
	fmt.Println("Async Consumer: Draining queue...")

	// 1. Wait for C++ Engine to process all pending requests
	idleCounter := 0
	for {
		var pendingInput C.uint64_t
		var activeWorkers C.uint64_t
		var pendingOutput C.uint64_t

		C.GetEngineStatus(&pendingInput, &activeWorkers, &pendingOutput)

		fmt.Printf("Engine Status - Input: %d, Active: %d, Output: %d\n", pendingInput, activeWorkers, pendingOutput)
		fmt.Printf("Metrics - Accepted: %d, Received: %d, SoldOut: %d, Committed: %d\n",
			atomic.LoadUint64(&AcceptedRequests),
			atomic.LoadUint64(&ResultsReceived),
			atomic.LoadUint64(&ResultsSoldOut),
			atomic.LoadUint64(&DBCommitted))

		if pendingInput == 0 && activeWorkers == 0 && pendingOutput == 0 {
			idleCounter++
			if idleCounter >= 5 { // Require stable idle state
				break
			}
		} else {
			idleCounter = 0
		}

		// While waiting, continue to consume results to prevent result queue from filling up
		for C.PollResult(&res) != 0 {
			order := model.Order{
				ID:      uint64(res.request_id),
				SkuID:   int64(res.sku_id),
				GuestID: uint64(res.guest_id),
				Qty:     int32(res.qty),
				Status:  1, // Created
			}
			atomic.AddUint64(&ResultsReceived, 1)
			orderChan <- order
		}
		time.Sleep(50 * time.Millisecond)
	}

	// 2. Wait a bit for in-flight requests (workers processing) to finish
	// Not needed as much with IsEngineIdle logic, but good for safety
	time.Sleep(100 * time.Millisecond)

	// 3. Final Drain of Result Queue
	for C.PollResult(&res) != 0 {
		order := model.Order{
			ID:      uint64(res.request_id),
			SkuID:   int64(res.sku_id),
			GuestID: uint64(res.guest_id),
			Qty:     int32(res.qty),
			Status:  1, // Created
		}
		atomic.AddUint64(&ResultsReceived, 1)
		orderChan <- order
	}
	fmt.Println("Async Consumer: Queue drained. Closing channel.")
	close(orderChan)
}

func dbWorker() {
	const batchSize = 1000
	var batch []model.Order = make([]model.Order, 0, batchSize)

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	defer close(dbWorkerDone)

	fmt.Println("Async DB Worker Started")

	for {
		select {
		case order, ok := <-orderChan:
			if !ok {
				// Channel closed, flush remaining
				if len(batch) > 0 {
					db.DB.Clauses(clause.OnConflict{DoNothing: true}).Create(&batch)
					atomic.AddUint64(&DBCommitted, uint64(len(batch)))
				}
				fmt.Println("Async DB Worker Stopped")
				return
			}
			batch = append(batch, order)
			if len(batch) >= batchSize {
				db.DB.Clauses(clause.OnConflict{DoNothing: true}).Create(&batch)
				atomic.AddUint64(&DBCommitted, uint64(len(batch)))
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				db.DB.Clauses(clause.OnConflict{DoNothing: true}).Create(&batch)
				atomic.AddUint64(&DBCommitted, uint64(len(batch)))
				batch = batch[:0]
			}
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
		status, err := cache.DeductInventory(req.SkuID, req.GuestID, req.Qty, req.RequestID)
		if err != nil {
			fmt.Printf("Redis Error: %v\n", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "System Error"})
			return
		}

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
	if C.EnqueueRequest(cReq) != 0 {
		atomic.AddUint64(&AcceptedRequests, 1)
		c.JSON(http.StatusAccepted, gin.H{"status": "queued", "request_id": reqID})
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
	c.JSON(http.StatusOK, gin.H{
		"accepted_requests": atomic.LoadUint64(&AcceptedRequests),
		"results_received":  atomic.LoadUint64(&ResultsReceived),
		"results_sold_out":  atomic.LoadUint64(&ResultsSoldOut),
		"db_committed":      atomic.LoadUint64(&DBCommitted),
		"queue_depth":       len(orderChan),
	})
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
