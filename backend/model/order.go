package model

import (
	"time"
)

type Order struct {
	ID        uint64    `gorm:"primaryKey;autoIncrement:false"` // Use RequestID as Primary Key (Idempotency)
	SkuID     int64     `gorm:"index"`
	GuestID   uint64    `gorm:"index"`
	Qty       int32
	Status    int       `gorm:"default:1"` // 1: Created, 2: Paid, 3: Cancelled
	CreatedAt time.Time `gorm:"autoCreateTime"`
}

func (Order) TableName() string {
	return "orders"
}
