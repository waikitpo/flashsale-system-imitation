package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	TargetURL = "http://localhost:3000/api/seckill/enqueue"
	SKU_ID    = 888 // Initial stock 5
	USER_ID   = 99999
)

func main() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	// 1. Reset Environment
	fmt.Println("Resetting environment for SKU 888 and User 99999...")
	rdb.Set(ctx, fmt.Sprintf("seckill:{%d}:stock", SKU_ID), 5, 0)
	rdb.Del(ctx, fmt.Sprintf("seckill:{%d}:bought", SKU_ID))
	rdb.Del(ctx, fmt.Sprintf("seckill:{%d}:pending", SKU_ID))
	
	// 2. Launch 50 Concurrent Requests for SAME User
	var wg sync.WaitGroup
	var successCount int32
	var duplicateCount int32
	var failCount int32

	fmt.Println("Launching 50 concurrent requests for the SAME user...")
	start := time.Now()

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			
			// Same Guest ID header ensures same user identity
			jsonBody := []byte(fmt.Sprintf(`{"sku_id":%d,"qty":1}`, SKU_ID))
			req, _ := http.NewRequest("POST", TargetURL, bytes.NewReader(jsonBody))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Guest-Id", fmt.Sprintf("%d", USER_ID))
			req.Header.Set("X-Request-Id", fmt.Sprintf("%d", time.Now().UnixNano()+int64(idx)))

			client := &http.Client{Timeout: 2 * time.Second}
			resp, err := client.Do(req)
			if err != nil {
				atomic.AddInt32(&failCount, 1)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == 202 {
				atomic.AddInt32(&successCount, 1)
			} else if resp.StatusCode == 409 { // Conflict / Duplicate
				atomic.AddInt32(&duplicateCount, 1)
			} else {
				atomic.AddInt32(&failCount, 1)
				fmt.Printf("Unexpected Status: %d\n", resp.StatusCode)
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	fmt.Printf("\n--- Test Results ---\n")
	fmt.Printf("Time Taken: %v\n", duration)
	fmt.Printf("Success (202): %d (Expected: 1)\n", successCount)
	fmt.Printf("Duplicate (409): %d (Expected: 49)\n", duplicateCount)
	fmt.Printf("Failures: %d\n", failCount)

	// 3. Verify Redis State
	stock, _ := rdb.Get(ctx, fmt.Sprintf("seckill:{%d}:stock", SKU_ID)).Int()
	bought, _ := rdb.SIsMember(ctx, fmt.Sprintf("seckill:{%d}:bought", SKU_ID), USER_ID).Result()

	fmt.Printf("Final Redis Stock: %d (Expected: 4, because 5-1=4)\n", stock)
	fmt.Printf("User in Bought Set: %v (Expected: true)\n", bought)

	if successCount == 1 && stock == 4 && bought {
		fmt.Println("\n✅ SUCCESS: System correctly prevented duplicate inventory deduction.")
	} else {
		fmt.Println("\n❌ FAILURE: Inconsistency detected.")
		os.Exit(1)
	}
}
