package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Config
const (
	TotalRequests = 100000
	Concurrency   = 1000
	TargetURL     = "http://localhost:3000/api/seckill/enqueue"
	WarmupCount   = 1000 // 预热请求数
)

// Stats
var (
	successCount int64
	failCount    int64 // Network errors
	status200    int64 // Actually 202 in our case
	status429    int64
	statusOther  int64
)

// 获取分位值（带边界保护）
func getPercentile(lats []int64, p int) int64 {
	if len(lats) == 0 {
		return 0
	}
	idx := int(float64(len(lats)) * float64(p) / 100.0)
	if idx >= len(lats) {
		idx = len(lats) - 1
	}
	return lats[idx]
}

func main() {
	// 1. Setup global HTTP Client with Keep-Alive
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 2000
	t.MaxConnsPerHost = 2000
	t.MaxIdleConnsPerHost = 2000
	t.IdleConnTimeout = 90 * time.Second

	client := &http.Client{
		Transport: t,
		Timeout:   2 * time.Second, // Tighter timeout
	}

	// Pre-allocate payload
	jsonBody := []byte(`{"sku_id":999,"qty":1}`)

	// Latency collector (buffered to avoid locking too much)
	latencies := make(chan int64, TotalRequests)

	var wg sync.WaitGroup
	// Semaphore to control concurrency
	sem := make(chan struct{}, Concurrency)

	fmt.Printf("Starting PRO benchmark: %d requests, concurrency %d\n", TotalRequests, Concurrency)
	fmt.Println("Warming up...")

	// 2. 预热请求（建立HTTP长连接，避免首屏延迟干扰）
	warmupBody := []byte(`{"sku_id":123,"qty":1}`) // Warmup with ample stock SKU
	for i := 0; i < WarmupCount; i++ {
		req, _ := http.NewRequest("POST", TargetURL, bytes.NewReader(warmupBody))
		req.Header.Set("Content-Type", "application/json")
		// Use special ID range for warmup to avoid polluting the 0-19999 range
		req.Header.Set("X-Request-Id", fmt.Sprintf("%d", 20000+i))
		req.Header.Set("X-Guest-Id", fmt.Sprintf("%d", rand.Int63()))

		resp, err := client.Do(req)
		if err == nil {
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
		}
	}

	start := time.Now()

	// 3. 主压测循环
	for i := 0; i < TotalRequests; i++ {
		wg.Add(1)
		sem <- struct{}{} // Acquire token

		go func(reqID int) {
			defer wg.Done()
			defer func() { <-sem }() // Release token

			reqStart := time.Now()

			// Reuse buffer reader
			req, err := http.NewRequest("POST", TargetURL, bytes.NewReader(jsonBody))
			if err != nil {
				fmt.Println("NewRequest error:", err)
				return
			}
			req.Header.Set("Content-Type", "application/json")
			// Unique Guest & Request ID（每个协程独立随机数生成器，避免竞争）
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(reqID)))
			req.Header.Set("X-Guest-Id", fmt.Sprintf("%d", r.Int63()))
			req.Header.Set("X-Request-Id", fmt.Sprintf("%d", reqID))

			if reqID == 0 {
				fmt.Println("Debug Req Headers:", req.Header)
			}

			resp, err := client.Do(req)
			duration := time.Since(reqStart).Microseconds()

			if err != nil {
				newFail := atomic.AddInt64(&failCount, 1)
				if newFail <= 5 {
					fmt.Printf("Request Error: %v\n", err)
				}
				return
			}

			// Always read and close body to ensure connection reuse
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()

			atomic.AddInt64(&successCount, 1)
			latencies <- duration

			switch resp.StatusCode {
			case http.StatusAccepted: // 202
				atomic.AddInt64(&status200, 1)
			case http.StatusTooManyRequests: // 429
				atomic.AddInt64(&status429, 1)
			default:
				atomic.AddInt64(&statusOther, 1)
			}
		}(i)
	}

	wg.Wait()
	close(latencies)
	totalDuration := time.Since(start)

	// 4. 统计延迟数据（预分配切片容量，减少扩容开销）
	var lats []int64 = make([]int64, 0, TotalRequests)
	for l := range latencies {
		lats = append(lats, l)
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })

	// 5. 输出压测报告
	fmt.Println("\n========================================")
	fmt.Printf("Benchmark Result (Client Side)\n")
	fmt.Println("========================================")
	fmt.Printf("Time Taken:       %v\n", totalDuration)
	fmt.Printf("Total Requests:   %d\n", TotalRequests)
	fmt.Printf("RPS (Throughput): %.2f req/s\n", float64(TotalRequests)/totalDuration.Seconds())
	fmt.Println("----------------------------------------")
	fmt.Printf("Network Errors:   %d\n", failCount)
	fmt.Printf("Success (Sent):   %d\n", successCount)
	fmt.Println("----------------------------------------")
	fmt.Printf("HTTP 202 (Enqueued): %d\n", status200)
	fmt.Printf("HTTP 429 (Rejected): %d\n", status429)
	fmt.Printf("HTTP Other:          %d\n", statusOther)
	fmt.Println("----------------------------------------")
	if len(lats) > 0 {
		fmt.Printf("Latency Min: %d µs\n", lats[0])
		fmt.Printf("Latency P50: %d µs\n", getPercentile(lats, 50))
		fmt.Printf("Latency P90: %d µs\n", getPercentile(lats, 90))
		fmt.Printf("Latency P99: %d µs\n", getPercentile(lats, 99))
		fmt.Printf("Latency Max: %d µs\n", lats[len(lats)-1])
	}
	fmt.Println("========================================")
}
