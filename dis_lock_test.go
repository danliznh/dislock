package dislock

import (
	"context"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"testing"
	"time"
)


var key = "__dis_lock_key_test__"

func TestObtain(t *testing.T) {
	conn, err:= redis.Dial("tcp", ":6379")
	if err != nil{
		t.Error(err)
		return
	}
	defer conn.Close()

	ctx := context.Background()
	dislock, err := Obtain(ctx, conn, key,
		time.Duration(10000*time.Millisecond),&Options{RetryStrategy: ExponentialBackoff(20*time.Millisecond, 3000*time.Millisecond)})
	if err != nil{
		t.Error(err)
		return
	}

	time.Sleep(6*time.Second)
	dislock.Release(ctx)

	time.Sleep(1*time.Second)

}


func TestSecondLockObtained(t *testing.T)  {
	conn, err:= redis.Dial("tcp", ":6379")
	if err != nil{
		t.Error(err)
		return
	}
	defer conn.Close()

	ctx := context.Background()
	dislock, err := Obtain(ctx, conn, key,
		time.Duration(10000*time.Millisecond),&Options{RetryStrategy: ExponentialBackoff(20*time.Millisecond, 3000*time.Millisecond)})
	if err != nil{
		t.Error(err)
		return
	}

	go func() {
		conn2, err:= redis.Dial("tcp", ":6379")
		if err != nil{
			t.Error(err)
			return
		}
		defer conn2.Close()
		start := time.Now()
		ctx2 := context.Background()
		disLock2, err2 := Obtain(ctx2, conn2, key,
			time.Duration(10000*time.Millisecond),&Options{RetryStrategy: ExponentialBackoff(20*time.Millisecond, 3000*time.Millisecond)})
		if err2 != nil{
			t.Error(err2)
			return
		}
		defer fmt.Printf("released :%v\n", disLock2.Token())
		defer disLock2.Release(ctx2)
		fmt.Printf("obtained:%v,%s\n", time.Since(start),disLock2.Token())
	}()
	time.Sleep(2*time.Second)
	dislock.Release(ctx)
	fmt.Printf("release\n")
	time.Sleep(1*time.Second)
	fmt.Printf("exit\n")




}


func TestSecondLockTimeout(t *testing.T)  {
	conn, err:= redis.Dial("tcp", ":6379")
	if err != nil{
		t.Error(err)
		return
	}
	defer conn.Close()

	ctx := context.Background()
	dislock, err := Obtain(ctx, conn, key,
		time.Duration(10000*time.Millisecond),&Options{RetryStrategy: ExponentialBackoff(20*time.Millisecond, 3000*time.Millisecond)})
	if err != nil{
		t.Error(err)
		return
	}

	go func() {
		conn2, err:= redis.Dial("tcp", ":6379")
		if err != nil{
			t.Error(err)
			return
		}
		defer conn2.Close()
		start := time.Now()
		ctx2 := context.Background()
		disLock2, err2 := Obtain(ctx2, conn2, key,
			time.Duration(3000*time.Millisecond),&Options{RetryStrategy: ExponentialBackoff(20*time.Millisecond, 3000*time.Millisecond)})
		if err2 != nil{
			fmt.Printf("obtaine timeout:%v, elapsed:%v\n",err2, time.Since(start))
			return
		}
		defer fmt.Printf("released :%v\n", disLock2.Token())
		defer disLock2.Release(ctx2)
		fmt.Printf("obtained:%v,%s\n", time.Since(start),disLock2.Token())
	}()
	time.Sleep(5*time.Second)
	dislock.Release(ctx)
	fmt.Printf("release\n")
	time.Sleep(1*time.Second)
	fmt.Printf("exit\n")




}
