package main

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"os"
)

func main() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	key := "seckill:stock:777"
	val, err := rdb.Get(ctx, key).Result()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Current Inventory: %s\n", val)
}
