package main

import (
	"fmt"
	"log"
	"os"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		dsn = "host=localhost user=user password=password dbname=seckill port=5432 sslmode=disable TimeZone=Asia/Shanghai"
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}

	var count int64
	// Assuming table name is 'orders'
	err = db.Table("orders").Count(&count).Error
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}

	fmt.Printf("Total orders in PostgreSQL: %d\n", count)
}
