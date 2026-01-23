package router

import (
	"net/http"

	"seckillapp/handler"

	"github.com/gin-gonic/gin"
)

func SetupRouter() *gin.Engine {
	// Start the consumer in background (for Phase A simulation)
	// handler.StartConsumer() // Removed duplicate call

	r := gin.New() // Use New() to control middlewares manually if needed, or Default()
	r.Use(gin.Recovery())
	// r.Use(gin.Logger()) // Disable logger for high performance benchmark if needed

	// Auth routes (Placeholder)
	auth := r.Group("api/auth")
	{
		auth.POST("/login", func(ctx *gin.Context) {
			ctx.AbortWithStatusJSON(http.StatusOK, gin.H{"msg": "Login successful"})
		})
		auth.POST("/register", func(ctx *gin.Context) {
			ctx.AbortWithStatusJSON(http.StatusOK, gin.H{"msg": "Register successful"})
		})
	}

	// Seckill Routes (Phase A)
	seckill := r.Group("api/seckill")
	{
		seckill.POST("/enqueue", handler.EnqueueHandler)
	}

	// Admin/Stats Routes
	admin := r.Group("api/admin")
	{
		admin.GET("/stats", handler.StatsHandler)
		admin.GET("/count", handler.CountHandler)
	}

	return r
}
