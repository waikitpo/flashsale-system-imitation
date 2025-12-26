package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	Rdb *redis.Client
	Ctx = context.Background()
)

// Lua Script for Atomic Check-and-Decrement
// KEYS[1]: Stock Key (e.g., seckill:stock:1001)
// KEYS[2]: Bought Set Key (e.g., seckill:bought:1001)
// ARGV[1]: User ID
// ARGV[2]: Quantity
const script = `
local stock_key = KEYS[1]
local bought_key = KEYS[2]
local user_id = ARGV[1]
local qty = tonumber(ARGV[2])

-- 1. Idempotency Check: Check if user already bought
if redis.call("SISMEMBER", bought_key, user_id) == 1 then
    return -1 -- Duplicate
end

-- 2. Stock Check
local stock = redis.call("GET", stock_key)
if not stock then
    return -2 -- Stock not initialized
end

stock = tonumber(stock)
if stock < qty then
    return 0 -- Sold out
end

-- 3. Deduct Stock
redis.call("DECRBY", stock_key, qty)

-- 4. Record Purchase
redis.call("SADD", bought_key, user_id)

return 1 -- Success
`

// Lua Script for Compensation (Rollback)
// KEYS[1]: Stock Key
// KEYS[2]: Bought Set Key
// ARGV[1]: User ID
// ARGV[2]: Quantity
const rollbackScript = `
local stock_key = KEYS[1]
local bought_key = KEYS[2]
local user_id = ARGV[1]
local qty = tonumber(ARGV[2])

-- 1. Remove Purchase Record
-- If remove fails (user not in set), we might still want to incr stock?
-- Let's assume strict pair: only rollback if user was in set.
if redis.call("SREM", bought_key, user_id) == 1 then
    redis.call("INCRBY", stock_key, qty)
    return 1 -- Rolled back
end

return 0 -- Nothing to rollback
`

func InitRedis(addr string, password string, db int) {
	Rdb = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	_, err := Rdb.Ping(Ctx).Result()
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to Redis: %v", err))
	}
	fmt.Println("Redis connected successfully.")
}

// WarmUpInventory initializes stock in Redis.
func WarmUpInventory(skuID int64, qty int) {
	key := fmt.Sprintf("seckill:stock:%d", skuID)
	err := Rdb.Set(Ctx, key, qty, 0).Err()
	if err != nil {
		fmt.Printf("Error warming up inventory for SKU %d: %v\n", skuID, err)
	} else {
		fmt.Printf("Redis: Warmed up SKU %d with %d items.\n", skuID, qty)
	}
	// Also clear the bought set for this SKU to ensure clean state
	Rdb.Del(Ctx, fmt.Sprintf("seckill:bought:%d", skuID))
}

// DeductInventory attempts to deduct stock atomically.
// Returns:
// 1: Success
// 0: Sold Out
// -1: Duplicate
// -2: Error (Not Initialized or Redis Error)
func DeductInventory(skuID int64, userID uint64, qty int) (int, error) {
	stockKey := fmt.Sprintf("seckill:stock:%d", skuID)
	boughtKey := fmt.Sprintf("seckill:bought:%d", skuID)

	val, err := Rdb.Eval(Ctx, script, []string{stockKey, boughtKey}, userID, qty).Result()
	if err != nil {
		return -2, err
	}

	return int(val.(int64)), nil
}

// GetInventory is for checking stock (e.g. for frontend display)
func GetInventory(skuID int64) (int, error) {
	key := fmt.Sprintf("seckill:stock:%d", skuID)
	val, err := Rdb.Get(Ctx, key).Int()
	if err == redis.Nil {
		return 0, nil
	}
	return val, err
}

// RollbackInventory reverts the deduction (Compensation).
// Includes Retry and Dead Letter Queue.
func RollbackInventory(skuID int64, userID uint64, qty int) error {
	stockKey := fmt.Sprintf("seckill:stock:%d", skuID)
	boughtKey := fmt.Sprintf("seckill:bought:%d", skuID)

	maxRetries := 3
	var err error

	for i := 0; i < maxRetries; i++ {
		_, err = Rdb.Eval(Ctx, rollbackScript, []string{stockKey, boughtKey}, userID, qty).Result()
		if err == nil {
			return nil
		}
		// Redis Error (Network, etc.)
		fmt.Printf("Rollback attempt %d failed: %v. Retrying...\n", i+1, err)
		time.Sleep(time.Duration(1<<i) * 50 * time.Millisecond) // Exponential Backoff
	}

	// Dead Letter Queue
	fmt.Printf("CRITICAL: Rollback failed permanently for User %d SKU %d. Logging to Dead Letter.\n", userID, skuID)
	errLog := fmt.Sprintf("%d:%d:%d:%d", skuID, userID, qty, time.Now().Unix())
	Rdb.RPush(Ctx, "seckill:dead_letter", errLog)

	return fmt.Errorf("rollback failed after retries: %w", err)
}

const PendingKey = "seckill:pending"

// MarkRequestPending adds request to In-Flight ZSET
func MarkRequestPending(skuID int64, userID uint64, qty int, reqID uint64) error {
	member := fmt.Sprintf("%d:%d:%d:%d", skuID, userID, qty, reqID)
	// Use UnixMicro for precision if needed, but Unix is fine for timeout > 1s
	return Rdb.ZAdd(Ctx, PendingKey, redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: member,
	}).Err()
}

// RemovePendingRequest removes request from In-Flight ZSET
func RemovePendingRequest(skuID int64, userID uint64, qty int, reqID uint64) error {
	member := fmt.Sprintf("%d:%d:%d:%d", skuID, userID, qty, reqID)
	return Rdb.ZRem(Ctx, PendingKey, member).Err()
}

// GetStalePendingRequests returns requests older than timeout
func GetStalePendingRequests(timeoutSec int64) ([]string, error) {
	limit := time.Now().Unix() - timeoutSec
	return Rdb.ZRangeByScore(Ctx, PendingKey, &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%d", limit),
	}).Result()
}
