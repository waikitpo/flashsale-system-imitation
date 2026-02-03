package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	TargetURL   = "http://localhost:3000/api/seckill/enqueue"
	StatsURL    = "http://localhost:3000/api/admin/stats"
	RedisAddr   = "127.0.0.1:6380"
	Stock       = 5000   // Stock per SKU   // Stock per SKU
	TotalReq    = 400000 // Mixed Workload
	Concurrency = 1000   // High concurrency
)

var SkuIDs = []int64{1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008}

func main() {
	fmt.Println("=== Starting Multi-SKU High Concurrency Benchmark ===")

	// 1. Initialize Redis
	rdb := redis.NewClient(&redis.Options{Addr: RedisAddr})
	ctx := context.Background()

	// 2. Reset State for all SKUs
	for _, skuID := range SkuIDs {
		fmt.Printf("Resetting SKU %d with stock %d...\n", skuID, Stock)
		rdb.Set(ctx, fmt.Sprintf("seckill:{%d}:stock", skuID), Stock, 0)
		rdb.Del(ctx, fmt.Sprintf("seckill:{%d}:bought", skuID))
		rdb.Del(ctx, fmt.Sprintf("seckill:{%d}:pending", skuID))
	}

	// 3. Warmup
	fmt.Println("Warming up connections...")
	runBatch(rdb, 100, 10, false)

	// Reset Stats
	initialStats := getStats()

	// 4. Run Benchmark
	fmt.Printf("\nRunning Benchmark: %d Requests, %d Concurrency, %d SKUs\n", TotalReq, Concurrency, len(SkuIDs))
	httpLatencies, queueLatencies := runBatch(rdb, TotalReq, Concurrency, true)

	// 5. Calculate Stats
	printStats("HTTP Response", httpLatencies)
	printStats("Queue Processing (Redis Pending)", queueLatencies)

	// Wait for DB Worker to finish
	waitForDBDrain(initialStats)

	// 6. Get C++ Internal Latency
	finalStats := getStats()
	printInternalLatency(initialStats, finalStats)
}

func waitForDBDrain(initialStats map[string]float64) {
	fmt.Println("Waiting for DB Worker to drain...")
	var lastCommitted float64 = -1
	stableCount := 0

	for i := 0; i < 100; i++ {
		stats := getStats()
		if stats == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		qDepth := stats["queue_depth"]
		committed := stats["db_committed"]
		received := stats["results_received"]

		if i%10 == 0 {
			fmt.Printf("Drain Status: QueueDepth=%.0f, Received=%.0f, Committed=%.0f\n", qDepth, received, committed)
		}

		if qDepth == 0 {
			if committed == lastCommitted && committed > 0 {
				stableCount++
				if stableCount >= 5 {
					break
				}
			} else if committed == 0 && received == 0 && i > 20 {
				break
			} else {
				stableCount = 0
			}
		} else {
			stableCount = 0
		}

		lastCommitted = committed
		time.Sleep(100 * time.Millisecond)
	}
}

func getStats() map[string]float64 {
	resp, err := http.Get(StatsURL)
	if err != nil {
		fmt.Println("Failed to get stats:", err)
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Println("Stats endpoint returned non-200 status:", resp.StatusCode)
		return nil
	}

	body, _ := io.ReadAll(resp.Body)
	var stats map[string]float64
	if err := json.Unmarshal(body, &stats); err != nil {
		fmt.Println("Failed to parse stats JSON:", err)
		return nil
	}
	return stats
}

func printInternalLatency(start, end map[string]float64) {
	if start == nil || end == nil {
		return
	}

	// Verify Actual DB Count
	resp, err := http.Get("http://localhost:3000/api/admin/count")
	var dbRealCount int64 = -1
	if err == nil {
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		var res map[string]int64
		json.Unmarshal(body, &res)
		dbRealCount = res["count"]
	}
	fmt.Printf("\n=== Persistence Verification ===\n")
	fmt.Printf("DB Real Row Count: %d\n", dbRealCount)
	fmt.Printf("DB Committed Counter (Stats): %.0f\n", end["db_committed"]-start["db_committed"])

	mpmcTotal := end["mpmc_latency_total"] - start["mpmc_latency_total"]
	mpmcCount := end["mpmc_count"] - start["mpmc_count"]

	spscTotal := end["spsc_latency_total"] - start["spsc_latency_total"]
	spscCount := end["spsc_count"] - start["spsc_count"]

	fmt.Println("\n=== Internal C++ Queue Latency (Avg) ===")
	if mpmcCount > 0 {
		avg := mpmcTotal / mpmcCount
		fmt.Printf("MPMC Queue (Input -> Dispatcher): %.2f us\n", avg/1000.0)
	}
	if spscCount > 0 {
		avg := spscTotal / spscCount
		fmt.Printf("SPSC Queue (Dispatcher -> Worker): %.2f us\n", avg/1000.0)
	}

	enqDiff := end["cpp_enqueue"] - start["cpp_enqueue"]
	deqDiff := end["cpp_dequeue"] - start["cpp_dequeue"]
	fmt.Printf("\n=== C++ Queue Flow Verification ===\n")
	fmt.Printf("Enqueue Count (Diff): %.0f\n", enqDiff)
	fmt.Printf("Dequeue Count (Diff): %.0f\n", deqDiff)

	dbBatchCount := end["db_batch_count"] - start["db_batch_count"]
	dbAvgLat := end["db_avg_latency_us"]

	fmt.Printf("\n=== DB Worker Metrics ===\n")
	fmt.Printf("Batch Count: %.0f\n", dbBatchCount)
	fmt.Printf("Avg Batch Latency: %.2f us\n", dbAvgLat)

	if dbBatchCount > 0 {
		dbCommitted := end["db_committed"] - start["db_committed"]
		avgBatchSize := dbCommitted / dbBatchCount
		fmt.Printf("Avg Batch Size: %.2f items\n", avgBatchSize)
	}
}

func runBatch(rdb *redis.Client, totalReq, concurrency int, record bool) ([]time.Duration, []time.Duration) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)
	latencies := make([]time.Duration, totalReq)
	queueLatencies := make([]time.Duration, 0)

	var (
		status202     int32
		status429     int32
		statusSoldOut int32 // 409
		statusOther   int32
		failCount     int32
	)

	// Create a single HTTP Client with connection pooling
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 2000
	t.MaxConnsPerHost = 2000
	t.MaxIdleConnsPerHost = 2000
	client := &http.Client{
		Transport: t,
		Timeout:   10 * time.Second,
	}

	// Request ID Strategy:
	// Use small IDs to trigger FULL BUSINESS LOGIC (WAL + Inventory + DB)
	// Range: 2000000000000000000 (2e18) - well below 18e18 threshold
	baseReqID := uint64(2000000000000000000)

	start := time.Now()

	for i := 0; i < totalReq; i++ {
		wg.Add(1)
		sem <- struct{}{}

		// Round-robin SKU selection
		skuID := SkuIDs[i%len(SkuIDs)]
		idx := i

		go func(skuID int64, idx int) {
			defer wg.Done()
			defer func() { <-sem }()

			// Optimized JSON creation
			// reqBody, _ := json.Marshal(...)
			jsonBody := []byte(fmt.Sprintf(`{"sku_id":%d,"qty":1}`, skuID))

			reqStart := time.Now()
			// Use http.NewRequest and client.Do to match high_concurrency benchmark style
			req, _ := http.NewRequest("POST", TargetURL, bytes.NewBuffer(jsonBody))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Guest-Id", fmt.Sprintf("%d", 1000+idx))
			req.Header.Set("X-Request-Id", fmt.Sprintf("%d", baseReqID+uint64(idx)))

			resp, err := client.Do(req)
			if err != nil {
				atomic.AddInt32(&failCount, 1)
				return
			}
			defer resp.Body.Close()

			duration := time.Since(reqStart)
			if record {
				latencies[idx] = duration
			}

			switch resp.StatusCode {
			case http.StatusAccepted:
				atomic.AddInt32(&status202, 1)
				if record {
					// Check queue latency (optional, requires redis check which is slow, so maybe skip or sample)
					// Skipping individual redis check for high performance benchmark
				}
			case http.StatusConflict:
				atomic.AddInt32(&statusSoldOut, 1)
			case http.StatusTooManyRequests, http.StatusServiceUnavailable:
				atomic.AddInt32(&status429, 1)
			default:
				atomic.AddInt32(&statusOther, 1)
			}
		}(skuID, idx)
	}

	wg.Wait()
	totalTime := time.Since(start)

	if record {
		rps := float64(totalReq) / totalTime.Seconds()
		fmt.Printf("Total Time: %v, RPS: %.2f\n", totalTime, rps)
		fmt.Println("----------------------------------------")
		fmt.Printf("HTTP 202 (Enqueued): %d\n", status202)
		fmt.Printf("HTTP 409 (Sold Out): %d\n", statusSoldOut)
		fmt.Printf("HTTP 429/503 (Queue Full): %d\n", status429)
		fmt.Printf("HTTP Other:          %d\n", statusOther)
		fmt.Printf("Network Errors:      %d\n", failCount)
		fmt.Println("----------------------------------------")
	}

	return latencies, queueLatencies
}

func printStats(name string, latencies []time.Duration) {
	if len(latencies) == 0 {
		fmt.Printf("\n=== %s: No Data ===\n", name)
		return
	}

	// Filter out zero values (timeouts/errors)
	var valid []float64
	for _, d := range latencies {
		if d > 0 {
			valid = append(valid, float64(d.Microseconds())/1000.0) // ms
		}
	}

	if len(valid) == 0 {
		return
	}

	sort.Float64s(valid)
	min := valid[0]
	max := valid[len(valid)-1]
	avg := 0.0
	for _, v := range valid {
		avg += v
	}
	avg /= float64(len(valid))

	p50 := valid[int(float64(len(valid))*0.50)]
	p90 := valid[int(float64(len(valid))*0.90)]
	p95 := valid[int(float64(len(valid))*0.95)]
	p99 := valid[int(float64(len(valid))*0.99)]

	fmt.Printf("\n=== Latency Statistics (%s) ===\n", name)
	fmt.Printf("Min: %.6fms\n", min)
	fmt.Printf("P50: %.6fms\n", p50)
	fmt.Printf("P90: %.6fms\n", p90)
	fmt.Printf("P95: %.6fms\n", p95)
	fmt.Printf("P99: %.6fms\n", p99)
	fmt.Printf("Max: %.6fms\n", max)
	fmt.Printf("Avg: %.6fms\n", avg)
}
