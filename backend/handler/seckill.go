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
	"seckillapp/db"
	"seckillapp/metrics"
	"seckillapp/model"
	"strconv"
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
)

// StartConsumer initializes the C++ engine.
func StartConsumer() {
	C.InitEngine()
	// Reset channels
	shutdownChan = make(chan struct{})
	dbWorkerDone = make(chan struct{})
	// Memory Buffer with Backpressure
	// 20000 is enough to hold all requests in memory if DB is slow,
	// but to demonstrate backpressure we could make it smaller.
	// Let's use 10000 to be safe but allow some backpressure if needed.
	orderChan = make(chan model.Order, 10000)

	go dbWorker()
	go processResults()
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
				order := model.Order{
					ID:      uint64(res.request_id),
					SkuID:   int64(res.sku_id),
					GuestID: uint64(res.guest_id),
					Qty:     int32(res.qty),
					Status:  1, // Created
				}
				// Push to Memory Buffer (Will block if full -> Backpressure)
				select {
				case orderChan <- order:
				case <-shutdownChan:
					fmt.Println("Async Consumer: Shutdown signal received while pushing")
					orderChan <- order
					break loop
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
				// Channel closed, flush remaining and exit
				if len(batch) > 0 {
					flushBatch(&batch)
				}
				fmt.Println("Async DB Worker: Exiting.")
				return
			}
			batch = append(batch, order)
			if len(batch) >= batchSize {
				flushBatch(&batch)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				flushBatch(&batch)
			}
		}
	}
}

func flushBatch(batch *[]model.Order) {
	if len(*batch) == 0 {
		return
	}

	// Bulk Insert
	// Clauses(clause.OnConflict{DoNothing: true}) handles potential duplicates if we re-consume logs
	// But since we use RequestID as Primary Key, we are safe.
	result := db.DB.Clauses(clause.OnConflict{DoNothing: true}).Create(batch)
	if result.Error != nil {
		// Log error, but don't stop.
		// In production, you might want to retry or send to a Dead Letter Queue (DLQ)
		// log.Printf("Failed to insert batch: %v", result.Error)
		println("Failed to insert batch:", result.Error.Error())
	} else {
		// log.Printf("Async DB: Flushed %d orders", len(*batch))
	}

	// Reset batch
	*batch = (*batch)[:0]
}

// StopConsumer stops the C++ engine.
func StopConsumer() {
	// 1. Wait for Engine Drained (Input=0, Workers=0)
	// Engine is still running and producing results.
	fmt.Println("Waiting for Engine to Drain...")
	C.WaitEngineDrained()
	fmt.Println("Engine Drained.")

	// 2. Continue draining result queue for a bit
	// processResults is still running.
	fmt.Println("Draining Result Queue (Wait 1s)...")
	time.Sleep(1 * time.Second)

	// 3. Stop Consumer (Signal shutdownChan)
	fmt.Println("Stopping Consumer...")
	close(shutdownChan)

	// 4. Wait for DB Worker
	<-dbWorkerDone
	fmt.Println("DB Worker Finished.")

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
	start := time.Now()
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

	// 3. Call C++ Engine
	var cReq C.CSeckillRequest
	cReq.sku_id = C.int64_t(req.SkuID)
	cReq.qty = C.int32_t(req.Qty)
	cReq.guest_id = C.uint64_t(req.GuestID)
	cReq.request_id = C.uint64_t(req.RequestID)

	// Non-blocking enqueue
	ret := C.EnqueueRequest(cReq)
	if ret == 1 {
		metrics.AddEnqueueOK()
		// Return 202 Accepted immediately
		c.JSON(http.StatusAccepted, gin.H{
			"msg":        "Enqueued",
			"request_id": reqID,
		})
	} else {
		metrics.AddEnqueueReject()
		// Queue full -> 429 Too Many Requests
		c.JSON(http.StatusTooManyRequests, gin.H{
			"error": "Queue full, please retry later",
		})
	}

	// 4. Record Latency
	metrics.AddLatency(time.Since(start).Nanoseconds())
}

func StatsHandler(c *gin.Context) {
	stats := metrics.GetStats()

	// Fetch real-time stats from C++ engine
	stats["queue_depth"] = uint64(C.GetQueueSize())
	stats["sold_total"] = uint64(C.GetSoldTotal())

	c.JSON(http.StatusOK, stats)
}

// BenchmarkEnqueue is a direct call helper for benchmarking without HTTP overhead
func BenchmarkEnqueue() {
	var cReq C.CSeckillRequest
	cReq.sku_id = 123
	cReq.qty = 1
	cReq.guest_id = 1001
	cReq.request_id = 2002

	C.EnqueueRequest(cReq)
}
