package main

import (
	"log"

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

func main() {
	_, err := enqueuer.Enqueue("greeting", work.Q{"name": "Dany M Pradana", "message": "How are you?"})
	if err != nil {
		log.Fatal(err)
	}
}
