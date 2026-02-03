package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"seckillapp/cache"
	"seckillapp/config"
	"seckillapp/db"
	"seckillapp/handler"
	"seckillapp/router"
	"syscall"
	"time"
)

func main() {
	config.InitConfig()
	// Override port from environment variable if present
	if envPort := os.Getenv("PORT"); envPort != "" {
		// Ensure it has a colon prefix if just a number is provided
		if envPort[0] != ':' {
			envPort = ":" + envPort
		}
		config.AppConfig.App.Port = envPort
	}
	db.InitDB() // Initialize Database

	// Initialize Redis
	// Default to 127.0.0.1:6380 to avoid IPv6 issues
	redisAddr := "127.0.0.1:6380"
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		redisAddr = addr
	} else if host := os.Getenv("REDIS_HOST"); host != "" {
		redisAddr = host + ":6379"
	}
	cache.InitRedis(redisAddr, "", 0)

	// Warm-up Inventory (Sync with C++ Engine)
	// In production, this should read from DB.
	cache.WarmUpInventory(888, 5)
	cache.WarmUpInventory(999, 100)

	r := router.SetupRouter()

	// Initialize C++ Engine
	handler.StartConsumer()

	// Perform System WarmUp
	handler.WarmUpSystem()

	// Setup graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	srv := &http.Server{
		Addr:    config.AppConfig.App.Port,
		Handler: r,
	}

	go func() {
		log.Printf("Server starting on %s\n", config.AppConfig.App.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()

	// log.Println("Server exiting...") // Confusing log
	<-quit

	// Cleanup C++ Engine
	// We need to export a StopEngine function in seckill.go/bridge.h
	// For now, let's rely on OS cleanup or add a stop function if available.
	// Actually, we added StopEngine in bridge.cpp, let's expose it.
	// handler.StopConsumer() // Moved to after shutdown

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal("Server forced to shutdown: ", err)
	}

	// Stop Consumer AFTER HTTP server stops accepting requests
	// This ensures no new requests are enqueued while we are draining
	log.Println("Stopping consumer and draining queue...")
	handler.StopConsumer()

	log.Println("Server exiting")
}
