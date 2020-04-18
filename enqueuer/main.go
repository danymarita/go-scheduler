package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gocraft/work"
	"github.com/gomodule/redigo/redis"
)

var redisPool = &redis.Pool{
	MaxActive: 5,
	MaxIdle:   5,
	Wait:      true,
	Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", ":6379")
	},
}

var enqueuer = work.NewEnqueuer("go_scheduler_namespace", redisPool)

type Handler struct {
	Enqueuer *work.Enqueuer
}

func NewHandler(enqueuer *work.Enqueuer) Handler {
	return Handler{
		Enqueuer: enqueuer,
	}
}

type GreetingRequest struct {
	Name    string `json:"name"`
	Message string `json:"message"`
}

func (h Handler) EnqueueGreeting(c *gin.Context) {
	var req GreetingRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.Name == "" || req.Message == "" {
		c.JSON(http.StatusBadRequest, gin.H{"status": "Name and Message are mandatory"})
		return
	}
	_, err := enqueuer.Enqueue("greeting", work.Q{"name": req.Name, "message": req.Message})
	if err != nil {
		log.Fatal(err)
		c.JSON(http.StatusInternalServerError, gin.H{"status": "Oops sorry try again"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "Thanks for send me message"})
}

func main() {
	router := gin.Default()
	handler := NewHandler(enqueuer)
	v1 := router.Group("/api/v1")
	{
		v1.POST("/enqueue", handler.EnqueueGreeting)
	}
	srv := &http.Server{
		Addr:    ":4500",
		Handler: router,
	}
	// Initializing the server in a goroutine so that
	// it won't block the graceful shutdown handling below
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown the server with
	// a timeout of 5 seconds.
	quit := make(chan os.Signal)
	// kill (no param) default send syscall.SIGTERM
	// kill -2 is syscall.SIGINT
	// kill -9 is syscall.SIGKILL but can't be catch, so don't need add it
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shuting down server...")

	// The context is used to inform the server it has 5 seconds to finish
	// the request it is currently handling
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}

	log.Println("Server exiting")
}
