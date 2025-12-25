package db

import (
	"log"
	"seckillapp/model"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var DB *gorm.DB

func InitDB() {
	var err error
	// Use SQLite for demo. In production, change to mysql.Open(dsn)
	// Enable WAL mode for SQLite to handle higher concurrency
	DB, err = gorm.Open(sqlite.Open("db/seckill.db?_journal_mode=WAL"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Error), // Silent logger for perf
	})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// Auto Migrate
	err = DB.AutoMigrate(&model.Order{})
	if err != nil {
		log.Fatalf("Failed to migrate database: %v", err)
	}
	
	log.Println("Database initialized successfully (SQLite)")
}
