package main

import (
	"fmt"
	"log"
	"seckillapp/model"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func main() {
	db, err := gorm.Open(sqlite.Open("db/seckill.db"), &gorm.Config{})
	if err != nil {
		log.Fatalf("failed to connect database: %v", err)
	}

	var count int64
	db.Model(&model.Order{}).Count(&count)
	fmt.Printf("Total orders in DB: %d\n", count)

	var minID, maxID int64
	db.Model(&model.Order{}).Select("MIN(id)").Scan(&minID)
	db.Model(&model.Order{}).Select("MAX(id)").Scan(&maxID)
	fmt.Printf("ID Range: %d - %d (Expected 0 - 19999)\n", minID, maxID)

	var orders []model.Order
	db.Limit(5).Find(&orders)
	fmt.Println("First 5 orders:")
	for _, order := range orders {
		fmt.Printf("- OrderID: %d, SkuID: %d, GuestID: %d, Qty: %d\n", order.ID, order.SkuID, order.GuestID, order.Qty)
	}
}
