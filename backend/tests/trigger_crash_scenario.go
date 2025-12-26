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
	SKU_ID    = 777
)

func main() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	// Check Initial Inventory
	key := fmt.Sprintf("seckill:stock:%d", SKU_ID)
	val, err := rdb.Get(ctx, key).Result()
	if err != nil {
		fmt.Printf("Error getting initial inventory: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Initial Inventory: %s\n", val)
	
	// Reset to 10 if needed
	if val != "10" {
		fmt.Println("Resetting inventory to 10...")
		rdb.Set(ctx, key, 10, 0)
		rdb.Del(ctx, fmt.Sprintf("seckill:bought:%d", SKU_ID))
		rdb.Del(ctx, "seckill:pending") // Clear pending too
	}

	// Send Request
	fmt.Println("Sending request for SKU 777...")
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

	time.Sleep(500 * time.Millisecond)
	val, _ = rdb.Get(ctx, key).Result()
	fmt.Printf("Inventory after request (should be 9): %s\n", val)
}
