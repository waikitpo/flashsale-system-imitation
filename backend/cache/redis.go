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
// KEYS[1]: Stock Key (e.g., seckill:{1001}:stock)
// KEYS[2]: Bought Set Key (e.g., seckill:{1001}:bought)
// KEYS[3]: Pending ZSet Key (e.g., seckill:{1001}:pending)
// ARGV[1]: User ID
// ARGV[2]: Quantity
// ARGV[3]: Timestamp
// ARGV[4]: Member String (sku:user:qty:reqID)
const script = `
local stock_key = KEYS[1]
local bought_key = KEYS[2]
local pending_key = KEYS[3]
local user_id = ARGV[1]
local qty = tonumber(ARGV[2])
local timestamp = ARGV[3]
local member = ARGV[4]

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

-- 5. Add to Pending Queue (Atomic with Stock Deduct)
redis.call("ZADD", pending_key, timestamp, member)

return 1 -- Success
`

// Lua Script for Compensation (Rollback)
// KEYS[1]: Stock Key
// KEYS[2]: Bought Set Key
// KEYS[3]: Pending ZSet Key
// ARGV[1]: User ID
// ARGV[2]: Quantity
// ARGV[3]: Member String
const rollbackScript = `
local stock_key = KEYS[1]
local bought_key = KEYS[2]
local pending_key = KEYS[3]
local user_id = ARGV[1]
local qty = tonumber(ARGV[2])
local member = ARGV[3]

-- 1. Remove Purchase Record
-- If remove fails (user not in set), we might still want to incr stock?
-- Let's assume strict pair: only rollback if user was in set.
if redis.call("SREM", bought_key, user_id) == 1 then
    redis.call("INCRBY", stock_key, qty)
    redis.call("ZREM", pending_key, member) -- Remove from pending too
    return 1 -- Rolled back
end

return 0 -- Nothing to rollback
`

func InitRedis(addr string, password string, db int) {
	Rdb = redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		ReadTimeout:  200 * time.Millisecond,
		WriteTimeout: 200 * time.Millisecond,
		PoolSize:     100,
		MinIdleConns: 10,
	})

	_, err := Rdb.Ping(Ctx).Result()
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to Redis: %v", err))
	}
	fmt.Println("Redis connected successfully.")
}

// WarmUpInventory initializes stock in Redis.
func WarmUpInventory(skuID int64, qty int) {
	key := fmt.Sprintf("seckill:{%d}:stock", skuID)
	err := Rdb.Set(Ctx, key, qty, 0).Err()
	if err != nil {
		fmt.Printf("Error warming up inventory for SKU %d: %v\n", skuID, err)
	} else {
		fmt.Printf("Redis: Warmed up SKU %d with %d items.\n", skuID, qty)
	}
	// Also clear the bought set for this SKU to ensure clean state
	Rdb.Del(Ctx, fmt.Sprintf("seckill:{%d}:bought", skuID))
	Rdb.Del(Ctx, fmt.Sprintf("seckill:{%d}:pending", skuID))
	// Register SKU for Sweeper
	Rdb.SAdd(Ctx, "seckill:skus", skuID)
}

// DeductInventory attempts to deduct stock atomically.
// Returns:
// 1: Success
// 0: Sold Out
// -1: Duplicate
// -2: Error (Not Initialized or Redis Error)
func DeductInventory(skuID int64, userID uint64, qty int, reqID uint64) (int, error) {
	stockKey := fmt.Sprintf("seckill:{%d}:stock", skuID)
	boughtKey := fmt.Sprintf("seckill:{%d}:bought", skuID)
	pendingKey := fmt.Sprintf("seckill:{%d}:pending", skuID)

	member := fmt.Sprintf("%d:%d:%d:%d", skuID, userID, qty, reqID)
	timestamp := float64(time.Now().Unix())

	val, err := Rdb.Eval(Ctx, script, []string{stockKey, boughtKey, pendingKey}, userID, qty, timestamp, member).Result()
	if err != nil {
		return -2, err
	}

	return int(val.(int64)), nil
}

// GetInventory is for checking stock (e.g. for frontend display)
func GetInventory(skuID int64) (int, error) {
	key := fmt.Sprintf("seckill:{%d}:stock", skuID)
	val, err := Rdb.Get(Ctx, key).Int()
	if err == redis.Nil {
		return 0, nil
	}
	return val, err
}

// RollbackInventory reverts the deduction (Compensation).
// Includes Retry and Dead Letter Queue.
func RollbackInventory(skuID int64, userID uint64, qty int, reqID uint64) error {
	stockKey := fmt.Sprintf("seckill:{%d}:stock", skuID)
	boughtKey := fmt.Sprintf("seckill:{%d}:bought", skuID)
	pendingKey := fmt.Sprintf("seckill:{%d}:pending", skuID)
	member := fmt.Sprintf("%d:%d:%d:%d", skuID, userID, qty, reqID)

	maxRetries := 3
	var err error

	for i := 0; i < maxRetries; i++ {
		_, err = Rdb.Eval(Ctx, rollbackScript, []string{stockKey, boughtKey, pendingKey}, userID, qty, member).Result()
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

// RemovePendingRequest removes request from In-Flight ZSET
func RemovePendingRequest(skuID int64, userID uint64, qty int, reqID uint64) error {
	pendingKey := fmt.Sprintf("seckill:{%d}:pending", skuID)
	member := fmt.Sprintf("%d:%d:%d:%d", skuID, userID, qty, reqID)
	return Rdb.ZRem(Ctx, pendingKey, member).Err()
}

// GetStalePendingRequests returns requests older than timeout
func GetStalePendingRequests(skuID int64, timeoutSec int64) ([]string, error) {
	pendingKey := fmt.Sprintf("seckill:{%d}:pending", skuID)
	limit := time.Now().Unix() - timeoutSec
	return Rdb.ZRangeByScore(Ctx, pendingKey, &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%d", limit),
	}).Result()
}
