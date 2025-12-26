package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"
)

const (
	TargetURL = "http://localhost:3000/api/seckill/enqueue"
	SKU_ID    = 888 // Test SKU with 5 inventory
)

func sendRequest(skuID int, qty int, name string) (int, error) {
	jsonBody := []byte(fmt.Sprintf(`{"sku_id":%d,"qty":%d}`, skuID, qty))
	req, err := http.NewRequest("POST", TargetURL, bytes.NewReader(jsonBody))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	// Unique IDs
	req.Header.Set("X-Guest-Id", fmt.Sprintf("%d", time.Now().UnixNano()))
	req.Header.Set("X-Request-Id", fmt.Sprintf("%d", time.Now().UnixNano()))

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	
	// Read body to clear buffer
	body, _ := ioutil.ReadAll(resp.Body)
	
	fmt.Printf("[%s] Qty=%d -> Status: %d, Body: %s\n", name, qty, resp.StatusCode, string(body))
	return resp.StatusCode, nil
}

func main() {
	fmt.Println("=== Starting Regression Test: Sold Out Flag Logic ===")
	fmt.Println("Target SKU: 888, Initial Inventory: 5")

	// Step 1: Oversize Request (Qty=10)
	// Expected: 409 Conflict (or 429 if queue full, but here queue is empty so likely processed by worker and rejected)
	// BUT CRITICALLY: It must NOT trigger the Sold Out Flag.
	fmt.Println("\nStep 1: Requesting 10 items (Should fail but NOT set flag)...")
	status, err := sendRequest(SKU_ID, 10, "Req_Oversize")
	if err != nil {
		panic(err)
	}
	// Note: Depending on implementation, worker might return 409 (Sold Out/Conflict) for "Not Enough Stock".
	// The key is whether subsequent requests work.
	
	// Wait a bit for worker to process
	time.Sleep(100 * time.Millisecond)

	// Step 2: Normal Request (Qty=1)
	// Expected: 202 Accepted. If Flag was set by Step 1, this will return 409 immediately.
	fmt.Println("\nStep 2: Requesting 1 item (Should succeed)...")
	status, err = sendRequest(SKU_ID, 1, "Req_Normal_1")
	if err != nil {
		panic(err)
	}
	
	if status == 409 {
		fmt.Println("❌ FAILED: Flag was incorrectly set by Oversize request!")
		os.Exit(1)
	} else if status == 202 {
		fmt.Println("✅ SUCCESS: Request accepted. Flag logic is correct.")
	} else {
		fmt.Printf("⚠️ WARNING: Unexpected status %d\n", status)
	}

	// Step 3: Drain remaining inventory (Remaining: 4)
	fmt.Println("\nStep 3: Draining remaining 4 items...")
	for i := 0; i < 4; i++ {
		sendRequest(SKU_ID, 1, fmt.Sprintf("Req_Drain_%d", i+1))
		time.Sleep(10 * time.Millisecond)
	}

	// Wait for processing
	time.Sleep(500 * time.Millisecond)

	// Step 4: Request after sold out
	// Expected: 409 (Sold Out) - Now the flag SHOULD be set
	fmt.Println("\nStep 4: Requesting 1 item after drain (Should be Sold Out)...")
	status, err = sendRequest(SKU_ID, 1, "Req_SoldOut")
	if status == 409 {
		fmt.Println("✅ SUCCESS: Correctly identified as Sold Out.")
	} else {
		fmt.Printf("❌ FAILED: Expected 409, got %d\n", status)
		os.Exit(1)
	}

	fmt.Println("\n=== Regression Test Passed ===")
}
