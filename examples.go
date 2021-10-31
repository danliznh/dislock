package dislock

import (
	"context"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"time"
)

var key = "__example_key__"

func examples() {

	conn, err := redis.Dial("tcp", ":6379")
	if err != nil {
		fmt.Printf("dial err:%v", err.Error())
		return
	}
	defer conn.Close()

	ctx := context.Background()
	dislock, err := Obtain(ctx, conn, key,
		time.Duration(10000*time.Millisecond), &Options{RetryStrategy: ExponentialBackoff(20*time.Millisecond, 3000*time.Millisecond)})
	if err != nil {
		fmt.Printf("obtain lock err:%v", err.Error())
		return
	}

	defer dislock.Release(ctx)

	time.Sleep(1 * time.Second)

}
