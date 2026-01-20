package handler_test

import (
	"seckillapp/db"
	"seckillapp/handler"
	"seckillapp/model"
	"testing"
	"time"
	"unsafe"
)

// TestEndToEndPersistence verifies that a request flows from Enqueue -> C++ Engine -> Go Worker -> SQLite
func TestEndToEndPersistence(t *testing.T) {
	// 1. Clean up DB (Delete all orders)
	if db.DB == nil {
		t.Fatal("DB not initialized")
	}
	db.DB.Exec("DELETE FROM orders")

	// 2. Prepare Request
	// Match C struct layout
	type CRequest struct {
		SkuID      int64
		Qty        int32
		_          int32
		GuestID    uint64
		RequestID  uint64
		TsIngress  int64
		TsPopMpmc  int64
		TsPushSpsc int64
		TsPopSpsc  int64
	}

	reqID := uint64(999999)
	req := CRequest{
		SkuID:     123,
		Qty:       1,
		GuestID:   8888,
		RequestID: reqID,
	}
	
	// 3. Enqueue
	ptr := unsafe.Pointer(&req)
	n := handler.EnqueueBatchRaw(ptr, 1)
	if n != 1 {
		t.Fatalf("Enqueue failed, returned %d", n)
	}

	// 4. Wait for async processing
	// C++ Dispatcher -> Worker (SPSC) -> Result Queue (MPMC) -> Go processResultWorker -> orderChan -> dbWorker -> SQLite
	// This might take a few milliseconds.
	timeout := time.After(2 * time.Second)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	found := false
	for {
		select {
		case <-timeout:
			t.Fatalf("Timeout waiting for Order %d to appear in DB", reqID)
		case <-ticker.C:
			var count int64
			db.DB.Model(&model.Order{}).Where("id = ?", reqID).Count(&count)
			if count > 0 {
				found = true
				break
			}
		}
		if found {
			break
		}
	}

	// 5. Verify Content
	var order model.Order
	db.DB.First(&order, reqID)
	
	if order.SkuID != 123 {
		t.Errorf("Expected SkuID 123, got %d", order.SkuID)
	}
	if order.GuestID != 8888 {
		t.Errorf("Expected GuestID 8888, got %d", order.GuestID)
	}
	if order.Status != 1 {
		t.Errorf("Expected Status 1, got %d", order.Status)
	}

	t.Logf("Successfully verified persistence for Order ID %d", reqID)
}
