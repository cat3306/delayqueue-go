# 基于redis延迟队列



## example

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/cat3306/delayqueue-go"
	"github.com/go-redis/redis/v8"
)

func main() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "facai888",
	})
	q := delayqueue.New(rdb, "delayqueue", func(err error, val string) {
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println(val)
	})
	q.Push(ctx, "10秒钟", 10, 1000)
	q.Push(ctx, "20秒钟", 10, 1000)
	q.Run()
	time.Sleep(time.Hour)
}

```
