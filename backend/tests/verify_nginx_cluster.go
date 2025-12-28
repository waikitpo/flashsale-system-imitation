package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// Point to Nginx Load Balancer
	TargetURL   = "http://localhost:8080/api/seckill/enqueue"
	SKU_ID      = 666 // Unique SKU for this cluster test
	STOCK       = 100
	TOTAL_REQ   = 1000 // 10x stock to ensure high concurrency
	CONCURRENCY = 50
)

var (
	successCount int32
	failCount    int32
	soldOutCount int32
)

func main() {
	fmt.Println("=== Starting Distributed Cluster Verification (Nginx -> 3x Backend) ===")

	// 1. Initialize Redis (Direct connection to check state)
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	ctx := context.Background()

	// 2. Reset State
	fmt.Println("Resetting Redis State...")
	rdb.Set(ctx, fmt.Sprintf("seckill:{%d}:stock", SKU_ID), STOCK, 0)
	rdb.Del(ctx, fmt.Sprintf("seckill:{%d}:bought", SKU_ID))
	rdb.Del(ctx, fmt.Sprintf("seckill:{%d}:pending", SKU_ID))

	// 3. Launch Attack
	fmt.Printf("Launching %d requests with concurrency %d...\n", TOTAL_REQ, CONCURRENCY)

	var wg sync.WaitGroup
	sem := make(chan struct{}, CONCURRENCY)
	start := time.Now()

	for i := 0; i < TOTAL_REQ; i++ {
		wg.Add(1)
		sem <- struct{}{}

		go func(id int) {
			defer wg.Done()
			defer func() { <-sem }()

			// Random User ID
			userID := 10000 + id

			jsonBody := []byte(fmt.Sprintf(`{"sku_id":%d,"qty":1}`, SKU_ID))
			req, _ := http.NewRequest("POST", TargetURL, bytes.NewReader(jsonBody))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Guest-Id", fmt.Sprintf("%d", userID))
			req.Header.Set("X-Request-Id", fmt.Sprintf("%d", time.Now().UnixNano()))

			// Use a short timeout to fail fast
			client := &http.Client{Timeout: 2 * time.Second}
			resp, err := client.Do(req)

			if err != nil {
				atomic.AddInt32(&failCount, 1)
				// fmt.Printf("Req Failed: %v\n", err)
				return
			}
			defer resp.Body.Close()
			io.Copy(ioutil.Discard, resp.Body)

			if resp.StatusCode == 202 {
				atomic.AddInt32(&successCount, 1)
			} else if resp.StatusCode == 409 {
				// Duplicate (Should not happen with unique user IDs, but possible if retry)
			} else if resp.StatusCode == 429 || resp.StatusCode == 503 {
				atomic.AddInt32(&soldOutCount, 1)
			} else {
				// 500 or 400
				atomic.AddInt32(&failCount, 1)
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	// 4. Verify Results
	fmt.Println("\n=== Test Completed ===")
	fmt.Printf("Time Taken: %v\n", duration)
	fmt.Printf("RPS: %.2f\n", float64(TOTAL_REQ)/duration.Seconds())
	fmt.Printf("Success (202): %d\n", successCount)
	fmt.Printf("Sold Out/Busy: %d\n", soldOutCount)
	fmt.Printf("Failures:      %d\n", failCount)

	// 5. Check Redis Consistency
	finalStock, _ := rdb.Get(ctx, fmt.Sprintf("seckill:{%d}:stock", SKU_ID)).Int()
	boughtSet, _ := rdb.SMembers(ctx, fmt.Sprintf("seckill:{%d}:bought", SKU_ID)).Result()

	fmt.Println("\n=== Consistency Check ===")
	fmt.Printf("Initial Stock: %d\n", STOCK)
	fmt.Printf("Final Stock:   %d (Expected: 0)\n", finalStock)
	fmt.Printf("Total Sold:    %d (Expected: %d)\n", len(boughtSet), STOCK)

	if finalStock == 0 && len(boughtSet) == STOCK {
		fmt.Println("\n✅ PASS: Cluster Consistency Verified!")
	} else {
		fmt.Println("\n❌ FAIL: Data Inconsistency Detected!")
		fmt.Printf("Oversold by: %d\n", len(boughtSet)-STOCK)
		os.Exit(1)
	}
}
