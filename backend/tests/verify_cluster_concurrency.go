package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	SKU_ID      = 888
	STOCK_COUNT = 100
	TOTAL_REQS  = 200   // 100 to A, 100 to B
	USER_ID_DUP = 12345 // The user who tries to double-dip
)

var (
	InstanceA = "http://localhost:3000/api/seckill/enqueue"
	InstanceB = "http://localhost:3001/api/seckill/enqueue"
)

func main() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	// 1. Reset Redis State
	fmt.Println("Initializing Redis state...")
	rdb.Set(ctx, fmt.Sprintf("seckill:{%d}:stock", SKU_ID), STOCK_COUNT, 0)
	rdb.Del(ctx, fmt.Sprintf("seckill:{%d}:bought", SKU_ID))
	rdb.Del(ctx, fmt.Sprintf("seckill:{%d}:pending", SKU_ID))
	// Clear the active SKU set for a clean slate, then add ours
	rdb.Del(ctx, "seckill:skus")
	rdb.SAdd(ctx, "seckill:skus", SKU_ID)

	// 2. Run Test
	var wg sync.WaitGroup
	var successCount int32
	var duplicateCount int32
	var failCount int32

	fmt.Printf("Starting %d concurrent requests to 2 instances...\n", TOTAL_REQS)

	for i := 0; i < TOTAL_REQS; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			// Determine Target Instance
			targetURL := InstanceA
			if idx%2 != 0 {
				targetURL = InstanceB
			}

			// Determine User ID
			// First 50 requests (idx 0-49) use the SAME user ID to test duplicate prevention
			// The rest use unique user IDs
			var userID uint64
			if idx < 50 {
				userID = USER_ID_DUP
			} else {
				userID = uint64(10000 + idx)
			}

			jsonBody := []byte(fmt.Sprintf(`{"sku_id":%d,"qty":1}`, SKU_ID))
			req, _ := http.NewRequest("POST", targetURL, bytes.NewReader(jsonBody))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Guest-Id", fmt.Sprintf("%d", userID))
			req.Header.Set("X-Request-Id", fmt.Sprintf("%d", time.Now().UnixNano()+int64(idx)))

			client := &http.Client{Timeout: 5 * time.Second}
			resp, err := client.Do(req)
			if err != nil {
				fmt.Printf("Request failed: %v\n", err)
				atomic.AddInt32(&failCount, 1)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == 202 || resp.StatusCode == 200 {
				atomic.AddInt32(&successCount, 1)
			} else if resp.StatusCode == 409 {
				atomic.AddInt32(&duplicateCount, 1)
			} else {
				fmt.Printf("Status %d\n", resp.StatusCode)
				atomic.AddInt32(&failCount, 1)
			}
		}(i)
	}

	wg.Wait()

	// 3. Verify Results
	fmt.Println("\n--- Test Results ---")
	fmt.Printf("Requests Sent: %d\n", TOTAL_REQS)
	fmt.Printf("Success: %d\n", successCount)
	fmt.Printf("Duplicate (409): %d\n", duplicateCount)
	fmt.Printf("Failed/Other: %d\n", failCount)

	// Check Redis Stock
	stock, _ := rdb.Get(ctx, fmt.Sprintf("seckill:{%d}:stock", SKU_ID)).Int()
	boughtSet, _ := rdb.SMembers(ctx, fmt.Sprintf("seckill:{%d}:bought", SKU_ID)).Result()

	fmt.Printf("Redis Stock Remaining: %d (Expected: %d)\n", stock, STOCK_COUNT-len(boughtSet))
	fmt.Printf("Actual Bought Count: %d\n", len(boughtSet))

	// Verify the specific duplicate user
	isMember, _ := rdb.SIsMember(ctx, fmt.Sprintf("seckill:{%d}:bought", SKU_ID), USER_ID_DUP).Result()
	if isMember {
		fmt.Printf("Duplicate User %d: Successfully bought (Correct)\n", USER_ID_DUP)
	} else {
		fmt.Printf("Duplicate User %d: Failed to buy (Unexpected if stock was available)\n", USER_ID_DUP)
	}

	// Logic Check
	// The first 50 requests were for the SAME user. Only 1 should succeed. 49 should be duplicates.
	// The other 150 requests were unique users.
	// Total Stock = 100.
	// Successes should be min(1 + 150, 100) = 100.
	// Actually wait, if stock is 100:
	// User 12345 takes 1. Stock -> 99.
	// 150 other users compete for 99 spots. 99 succeed.
	// Total successes should be 100.
	// Total duplicates (for user 12345) should be roughly 49 (depending on race conditions, some might fail with sold out if they came late, but here stock is enough).

	// Wait, actually stock is 100.
	// We send 50 requests for User A.
	// We send 150 requests for unique users.
	// Total unique users = 1 + 150 = 151.
	// Total stock = 100.
	// So 100 people should succeed. 51 should get "Sold Out" (or 49 duplicates for User A + others sold out).

	if int(successCount) == 100 && stock == 0 {
		fmt.Println("✅ PASSED: Inventory exhausted correctly, no over-selling.")
	} else if int(successCount) < 100 && stock > 0 {
		fmt.Println("⚠️  WARNING: Stock remains but requests stopped/failed?")
	} else {
		fmt.Printf("❌ FAILED: Success count %d does not match expected limit.\n", successCount)
	}

}
