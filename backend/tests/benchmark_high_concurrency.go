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
	RedisAddr   = "localhost:6380"
	SkuID       = 777    // Special SKU for latency test
	Stock       = 5000    // Enough stock to avoid early sold out
	TotalReq    = 200000 // Total requests
	Concurrency = 1000   // High concurrency (reduced from 1000)
)

func main() {
	fmt.Println("=== Starting High Concurrency Benchmark ===")

	// 1. Initialize Redis
	rdb := redis.NewClient(&redis.Options{Addr: RedisAddr})
	ctx := context.Background()

	// 2. Reset State
	fmt.Printf("Resetting SKU %d with stock %d...\n", SkuID, Stock)
	rdb.Set(ctx, fmt.Sprintf("seckill:{%d}:stock", SkuID), Stock, 0)
	rdb.Del(ctx, fmt.Sprintf("seckill:{%d}:bought", SkuID))
	rdb.Del(ctx, fmt.Sprintf("seckill:{%d}:pending", SkuID))

	// 3. Warmup
	fmt.Println("Warming up connections...")
	runBatch(rdb, 100, 10, SkuID, false)

	// Reset Stats
	initialStats := getStats()

	// 4. Run Benchmark
	fmt.Printf("\nRunning Benchmark: %d Requests, %d Concurrency\n", TotalReq, Concurrency)
	httpLatencies, queueLatencies := runBatch(rdb, TotalReq, Concurrency, SkuID, true)

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
	fmt.Printf("DB Batch Latency P50: %.2f us\n", end["db_latency_p50"])
	fmt.Printf("DB Batch Latency P90: %.2f us\n", end["db_latency_p90"])
	fmt.Printf("DB Batch Latency P99: %.2f us\n", end["db_latency_p99"])
	fmt.Printf("DB Batch Latency Max: %.2f us\n", end["db_latency_max"])

	if dbBatchCount > 0 {
		dbCommitted := end["db_committed"] - start["db_committed"]
		avgBatchSize := dbCommitted / dbBatchCount
		fmt.Printf("Avg Batch Size: %.2f items\n", avgBatchSize)
		fmt.Printf("Batch Size P50: %.0f items\n", end["db_batch_size_p50"])
		fmt.Printf("Batch Size P90: %.0f items\n", end["db_batch_size_p90"])
		fmt.Printf("Batch Size P99: %.0f items\n", end["db_batch_size_p99"])
		fmt.Printf("Batch Size Max: %.0f items\n", end["db_batch_size_max"])
	}

	cleanupBatchCount := end["cleanup_batch_count"] - start["cleanup_batch_count"]
	cleanupAvgSize := end["cleanup_avg_size"]
	fmt.Printf("\n=== Redis Cleanup Metrics ===\n")
	fmt.Printf("Batch Count: %.0f\n", cleanupBatchCount)
	fmt.Printf("Avg Batch Size: %.2f items\n", cleanupAvgSize)
}

func runBatch(rdb *redis.Client, totalReq, concurrency int, skuID int64, record bool) ([]time.Duration, []time.Duration) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)
	latencies := make([]time.Duration, totalReq)
	queueLatencies := make([]time.Duration, 0)

	var (
		status202     int32
		status429     int32
		statusSoldOut int32 // 409 + "Sold out" in body or specific code
		statusOther   int32
		failCount     int32
	)

	// Create a single HTTP Client with connection pooling
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 2000
	t.MaxConnsPerHost = 2000
	t.MaxIdleConnsPerHost = 2000
	client := &http.Client{
		Timeout:   10 * time.Second,
		Transport: t,
	}

	start := time.Now()

	for i := 0; i < totalReq; i++ {
		wg.Add(1)
		sem <- struct{}{}

		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()

			guestID := 200000 + idx
			reqID := uint64(time.Now().UnixNano()) + uint64(idx)

			jsonBody := []byte(fmt.Sprintf(`{"sku_id":%d,"qty":1}`, skuID))
			req, _ := http.NewRequest("POST", TargetURL, bytes.NewReader(jsonBody))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Guest-Id", fmt.Sprintf("%d", guestID))
			req.Header.Set("X-Request-Id", fmt.Sprintf("%d", reqID))

			reqStart := time.Now()
			resp, err := client.Do(req)
			duration := time.Since(reqStart)

			if record {
				latencies[idx] = duration
			}

			if err == nil {
				// Check Status Code
				switch resp.StatusCode {
				case 200, 202:
					atomic.AddInt32(&status202, 1)
				case 409:
					// Read body to distinguish Sold Out vs Duplicate (if applicable)
					// In our handler:
					// - 409 Conflict + "Sold out" -> Sold Out
					// - 409 Conflict + "Duplicate" -> Duplicate (if implemented)
					// Actually our handler returns 409 for Sold Out.
					// Let's assume 409 is mostly Sold Out for unique users.
					atomic.AddInt32(&statusSoldOut, 1)
				case 429, 503: // Service Unavailable / Queue Full
					atomic.AddInt32(&status429, 1)
				default:
					atomic.AddInt32(&statusOther, 1)
				}

				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			} else {
				atomic.AddInt32(&failCount, 1)
			}
		}(i)
	}

	wg.Wait()

	totalDuration := time.Since(start)

	if record {
		rps := float64(totalReq) / totalDuration.Seconds()
		fmt.Printf("Total Time: %v, RPS: %.2f\n", totalDuration, rps)
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
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	count := len(latencies)
	if count == 0 {
		fmt.Printf("\n=== %s: No Data ===\n", name)
		return
	}

	p50 := latencies[int(float64(count)*0.50)]
	p90 := latencies[int(float64(count)*0.90)]
	p95 := latencies[int(float64(count)*0.95)]
	p99 := latencies[int(float64(count)*0.99)]
	min := latencies[0]
	max := latencies[count-1]

	var sum time.Duration
	for _, d := range latencies {
		sum += d
	}
	avg := sum / time.Duration(count)

	fmt.Printf("\n=== Latency Statistics (%s) ===\n", name)
	fmt.Printf("Min: %v\n", min)
	fmt.Printf("P50: %v\n", p50)
	fmt.Printf("P90: %v\n", p90)
	fmt.Printf("P95: %v\n", p95)
	fmt.Printf("P99: %v\n", p99)
	fmt.Printf("Max: %v\n", max)
	fmt.Printf("Avg: %v\n", avg)
}
