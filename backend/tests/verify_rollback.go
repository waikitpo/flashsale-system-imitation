package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	TargetURL = "http://localhost:3000/api/seckill/enqueue"
	SKU_ID    = 777 // Configured in main.go to have 10 items, but NOT in C++
)

func main() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	// Check Initial Inventory
	key := fmt.Sprintf("seckill:{%d}:stock", SKU_ID)
	val, err := rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		fmt.Println("Initializing SKU 777 to 10 for test...")
		rdb.Set(ctx, key, 10, 0)
		rdb.Del(ctx, fmt.Sprintf("seckill:{%d}:bought", SKU_ID))
		val = "10"
		err = nil
	}
	if err != nil {
		fmt.Printf("Error getting initial inventory: %v\n", err)
		fmt.Println("Ensure backend is running and initialized with SKU 777.")
		os.Exit(1)
	}
	fmt.Printf("Initial Inventory for SKU %d: %s\n", SKU_ID, val)
	if val != "10" {
		fmt.Printf("Expected 10, got %s. Did you restart the backend with WarmUpInventory(777, 10)?\n", val)
		os.Exit(1)
	}

	// Send Request
	fmt.Println("Sending request for SKU 777 (Should fail in C++ and Rollback)...")

	jsonBody := []byte(fmt.Sprintf(`{"sku_id":%d,"qty":1}`, SKU_ID))
	req, _ := http.NewRequest("POST", TargetURL, bytes.NewReader(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Guest-Id", fmt.Sprintf("%d", time.Now().UnixNano()))
	req.Header.Set("X-Request-Id", fmt.Sprintf("%d", time.Now().UnixNano()))

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	fmt.Printf("HTTP Status: %d\n", resp.StatusCode)
	if resp.StatusCode != 202 {
		fmt.Printf("Expected 202, got %d\n", resp.StatusCode)
		os.Exit(1)
	}

	// At this point, Redis decremented to 9 (synchronously in handler).
	// Then C++ failed (async).
	// Then Rollback happened (async).
	// So we wait a bit and check Redis.

	fmt.Println("Waiting for async processing...")
	time.Sleep(1 * time.Second)

	val, err = rdb.Get(ctx, key).Result()
	if err != nil {
		fmt.Printf("Error getting final inventory: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Final Inventory for SKU %d: %s\n", SKU_ID, val)

	if val == "10" {
		fmt.Println("✅ SUCCESS: Inventory rolled back to 10.")
	} else if val == "9" {
		fmt.Println("❌ FAILED: Inventory stayed at 9 (Rollback failed).")
		os.Exit(1)
	} else {
		fmt.Printf("⚠️ WARNING: Unexpected inventory %s\n", val)
		os.Exit(1)
	}
}
