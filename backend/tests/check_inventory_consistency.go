package main

import (
	"context"
	"fmt"
	"os"

	"github.com/redis/go-redis/v9"
)

const (
	RedisAddr = "localhost:6379"
	SkuID     = 777
	Stock     = 500
)

func main() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: RedisAddr,
	})

	fmt.Println("=== Verifying Inventory Consistency ===")

	// 1. Check Redis Stock
	keyStock := fmt.Sprintf("seckill:{%d}:stock", SkuID)
	valStock, err := rdb.Get(ctx, keyStock).Int()
	if err != nil && err != redis.Nil {
		fmt.Printf("Error getting stock: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Current Redis Stock: %d (Expected: 0)\n", valStock)

	// 2. Check Bought Set Count
	keyBought := fmt.Sprintf("seckill:{%d}:bought", SkuID)
	valBought, err := rdb.SCard(ctx, keyBought).Result()
	if err != nil {
		fmt.Printf("Error getting bought count: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Total Sold (Redis Set): %d (Expected: %d)\n", valBought, Stock)

	// 3. Consistency Check
	if valStock == 0 && int(valBought) == Stock {
		fmt.Println("\n✅ SUCCESS: Inventory is consistent. Exactly 500 items sold.")
	} else {
		fmt.Println("\n❌ FAILURE: Inventory mismatch!")
		if valStock != 0 {
			fmt.Printf("  - Oversold/Undersold? Stock remaining: %d\n", valStock)
		}
		if int(valBought) != Stock {
			fmt.Printf("  - Sold count mismatch: %d\n", valBought)
		}
		// Check for Negative Stock (Overselling)
		if valStock < 0 {
			fmt.Printf("  - CRITICAL: Stock is negative! Oversold by %d\n", -valStock)
		}
		os.Exit(1)
	}
}
