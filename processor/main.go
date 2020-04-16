package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

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

type Context struct {
	currentTime time.Time
}

func main() {
	// Make a new pool. Arguments:
	// Context{} is a struct that will be the context for the request.
	// 10 is the max concurrency
	// "go_scheduler_namespace" is the Redis namespace
	// redisPool is a Redis pool
	pool := work.NewWorkerPool(Context{}, 10, "go_scheduler_namespace", redisPool)

	// Add middleware that will be executed for each job
	pool.Middleware((*Context).GetTime)
	pool.Middleware((*Context).Log)

	// Map the name of jobs to handler functions
	pool.Job("greeting", (*Context).Greeting)

	// Customize options:
	pool.JobWithOptions("export", work.JobOptions{Priority: 10, MaxFails: 1}, (*Context).Export)

	// Start processing jobs
	pool.Start()

	// Wait for a signal to quit:
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, os.Kill)
	<-signalChan

	// Stop the pool
	pool.Stop()
}

func (c *Context) GetTime(job *work.Job, next work.NextMiddlewareFunc) error {
	// assign currentTime for future middleware and handlers to use.
	c.currentTime = time.Now()
	return next()
}

func (c *Context) Log(job *work.Job, next work.NextMiddlewareFunc) error {
	fmt.Printf("Starting job: %s at %v \n", job.Name, c.currentTime.Format(time.RFC3339))
	return next()
}

func (c *Context) Greeting(job *work.Job) error {
	// Extract arguments:
	name := job.ArgString("name")
	message := job.ArgString("message")
	if err := job.ArgError(); err != nil {
		return err
	}

	fmt.Printf("Hello %s, %s\n", name, message)
	return nil
}

func (c *Context) Export(job *work.Job) error {
	return nil
}
