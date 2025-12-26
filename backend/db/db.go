package db

import (
	"log"
	"os"
	"seckillapp/model"

	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var DB *gorm.DB

func InitDB() {
	var err error

	// Check if PG_DSN environment variable is set
	dsn := os.Getenv("PG_DSN")
	usePG := os.Getenv("USE_PG")

	// If PG_DSN is set OR USE_PG=true, try PostgreSQL
	if dsn != "" || usePG == "true" {
		if dsn == "" {
			dsn = "host=localhost user=user password=password dbname=seckill port=5432 sslmode=disable TimeZone=Asia/Shanghai"
		}
		DB, err = gorm.Open(postgres.Open(dsn), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Error),
		})
		if err != nil {
			log.Fatalf("Failed to connect to PostgreSQL: %v. \nHint: Make sure PostgreSQL is running or set correct PG_DSN.", err)
		}
		log.Println("Database initialized successfully (PostgreSQL)")
	} else {
		// Fallback to SQLite (Default for Dev/Test without PG)
		log.Println("PG_DSN not set and USE_PG!=true. Falling back to SQLite for local development.")
		DB, err = gorm.Open(sqlite.Open("db/seckill.db?_journal_mode=WAL"), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Error),
		})
		if err != nil {
			log.Fatalf("Failed to connect to SQLite: %v", err)
		}
		log.Println("Database initialized successfully (SQLite)")
	}

	// Auto Migrate
	err = DB.AutoMigrate(&model.Order{})
	if err != nil {
		log.Fatalf("Failed to migrate database: %v", err)
	}
}
