package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	TargetURL   = "http://localhost:3000/api/seckill/enqueue"
	StatsURL    = "http://localhost:3000/api/admin/stats"
	RedisAddr   = "localhost:6379"
	SkuID       = 777   // Special SKU for latency test
	Stock       = 50000 // Large stock to avoid sold-out during latency test
	TotalReq    = 5000
	Concurrency = 100 // Simulating 100 concurrent users
)

func main() {
	fmt.Println("=== Starting Latency Benchmark ===")

	// 1. Initialize Redis
	rdb := redis.NewClient(&redis.Options{Addr: RedisAddr})
	ctx := context.Background()

	// 2. Reset State
	fmt.Printf("Resetting SKU %d with stock %d...\n", SkuID, Stock)
	rdb.Set(ctx, fmt.Sprintf("seckill:{%d}:stock", SkuID), Stock, 0)
	rdb.Del(ctx, fmt.Sprintf("seckill:{%d}:bought", SkuID))
	rdb.Del(ctx, fmt.Sprintf("seckill:{%d}:pending", SkuID))

	// 3. Warmup (Optional, but good for JIT/Connection Pools)
	fmt.Println("Warming up connections...")
	runBatch(rdb, 100, 10, SkuID, false) // 100 reqs, 10 concurrency

	// Reset Stats before benchmark
	// Note: We don't have an API to reset stats, so we record initial values
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
	// Wait until queue_depth is 0 and db_committed stops changing
	var lastCommitted float64 = -1
	stableCount := 0

	for i := 0; i < 100; i++ { // Max 10 seconds
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
			if committed == lastCommitted && committed > 0 { // Ensure we committed something
				stableCount++
				if stableCount >= 5 { // Stable for 500ms
					break
				}
			} else if committed == 0 && received == 0 && i > 20 {
				// If nothing received after 2 seconds, give up
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
		fmt.Println("Raw body:", string(body))
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
	} else {
		fmt.Println("MPMC Queue: No Data")
	}

	if spscCount > 0 {
		avg := spscTotal / spscCount
		fmt.Printf("SPSC Queue (Dispatcher -> Worker): %.2f us\n", avg/1000.0)
	} else {
		fmt.Println("SPSC Queue: No Data")
	}

	// Print New Counters
	enqDiff := end["cpp_enqueue"] - start["cpp_enqueue"]
	deqDiff := end["cpp_dequeue"] - start["cpp_dequeue"]
	fmt.Printf("\n=== C++ Queue Flow Verification ===\n")
	fmt.Printf("Enqueue Count (Diff): %.0f\n", enqDiff)
	fmt.Printf("Dequeue Count (Diff): %.0f\n", deqDiff)
	if enqDiff > 0 && deqDiff > 0 {
		fmt.Printf("Status: Traffic Flowing OK (Enqueue=%.0f, Dequeue=%.0f)\n", enqDiff, deqDiff)
	} else {
		fmt.Printf("Status: No Flow Detected (Check Enqueue/Dequeue Logic)\n")
	}

	// Print DB Metrics
	dbBatchCount := end["db_batch_count"] - start["db_batch_count"]
	dbAvgLat := end["db_avg_latency_us"] // Average is already calculated in handler, just take latest

	fmt.Printf("\n=== DB Worker Metrics ===\n")
	fmt.Printf("Batch Count: %.0f\n", dbBatchCount)
	fmt.Printf("Avg Batch Latency: %.2f us\n", dbAvgLat)
	fmt.Printf("DB Batch Latency P50: %.2f us\n", end["db_latency_p50"])
	fmt.Printf("DB Batch Latency P90: %.2f us\n", end["db_latency_p90"])
	fmt.Printf("DB Batch Latency P99: %.2f us\n", end["db_latency_p99"])
	fmt.Printf("DB Batch Latency Max: %.2f us\n", end["db_latency_max"])

	if dbBatchCount > 0 {
		// Estimated Total Items = DBCommitted Diff
		dbCommitted := end["db_committed"] - start["db_committed"]
		avgBatchSize := dbCommitted / dbBatchCount
		fmt.Printf("Avg Batch Size: %.2f items\n", avgBatchSize)
		fmt.Printf("Batch Size P50: %.0f items\n", end["db_batch_size_p50"])
		fmt.Printf("Batch Size P90: %.0f items\n", end["db_batch_size_p90"])
		fmt.Printf("Batch Size P99: %.0f items\n", end["db_batch_size_p99"])
		fmt.Printf("Batch Size Max: %.0f items\n", end["db_batch_size_max"])
	}

	// Print Cleanup Metrics
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
	queueLatencies := make([]time.Duration, 0, totalReq)
	var queueLatenciesLock sync.Mutex

	// Queue Monitoring
	activeReqs := sync.Map{} // reqID (uint64) -> startTime (time.Time)
	monitorDone := make(chan struct{})

	if record {
		go func() {
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			pendingKey := fmt.Sprintf("seckill:{%d}:pending", skuID)
			ctx := context.Background()

			for {
				select {
				case <-monitorDone:
					return
				case <-ticker.C:
					// Get all pending requests
					members, err := rdb.ZRange(ctx, pendingKey, 0, -1).Result()
					if err != nil {
						continue
					}

					// Build set of currently pending IDs
					pendingSet := make(map[uint64]bool)
					for _, m := range members {
						// Format: sku:guest:qty:reqID
						parts := strings.Split(m, ":")
						if len(parts) == 4 {
							if rid, err := strconv.ParseUint(parts[3], 10, 64); err == nil {
								pendingSet[rid] = true
							}
						}
					}

					// Check active requests
					activeReqs.Range(func(key, value interface{}) bool {
						rid := key.(uint64)
						startTime := value.(time.Time)

						// If NOT in pending set, it is processed (or rejected/lost, but we assume processed for 202 OK)
						if !pendingSet[rid] {
							// Double check: Make sure we don't count it if it was NEVER in pending (e.g. just added).
							// But we add to activeReqs AFTER HTTP return. So it should have been in pending.
							// If it's not there, it's done.

							duration := time.Since(startTime)
							queueLatenciesLock.Lock()
							queueLatencies = append(queueLatencies, duration)
							queueLatenciesLock.Unlock()

							activeReqs.Delete(rid)
						}
						return true
					})
				}
			}
		}()
	}

	start := time.Now()

	for i := 0; i < totalReq; i++ {
		wg.Add(1)
		sem <- struct{}{}

		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()

			// Random-ish Guest ID to avoid duplicate blocks
			guestID := 200000 + idx
			// Unique Request ID
			reqID := uint64(time.Now().UnixNano()) + uint64(idx)

			jsonBody := []byte(fmt.Sprintf(`{"sku_id":%d,"qty":1}`, skuID))
			req, _ := http.NewRequest("POST", TargetURL, bytes.NewReader(jsonBody))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Guest-Id", fmt.Sprintf("%d", guestID))
			req.Header.Set("X-Request-Id", fmt.Sprintf("%d", reqID))

			client := &http.Client{Timeout: 5 * time.Second}

			reqStart := time.Now()
			resp, err := client.Do(req)
			duration := time.Since(reqStart)

			if record {
				latencies[idx] = duration
			}

			if err == nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()

				// If Accepted, track for Queue Latency
				if record && (resp.StatusCode == 200 || resp.StatusCode == 202) {
					// We use reqStart as the approximation of Enqueue Time (Client Side)
					// Or better: Use time.Now() (After HTTP return) is NOT Enqueue Time.
					// Enqueue Time was slightly before HTTP return.
					// If we want "Total Latency" (User perspective): use reqStart.
					// If we want "Queue Latency" (System perspective): use time.Now() (post-enqueue).
					// User usually cares about "When do I get the result?".
					// So "Total Async Latency" = Time from Request to Result.
					// So use reqStart.
					activeReqs.Store(reqID, reqStart)
				}
			}
		}(i)
	}

	wg.Wait()

	// Wait a bit for queue to drain
	if record {
		fmt.Println("Waiting for queue to drain...")
		// Give it up to 5 seconds to drain
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			empty := true
			activeReqs.Range(func(_, _ interface{}) bool {
				empty = false
				return false
			})
			if empty {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		close(monitorDone)
	}

	totalDuration := time.Since(start)

	if record {
		rps := float64(totalReq) / totalDuration.Seconds()
		fmt.Printf("Total Time: %v, RPS: %.2f\n", totalDuration, rps)
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
