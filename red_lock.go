package dislock

import (
	"context"
	"github.com/gomodule/redigo/redis"
	"net"
	"sync"
	"time"
)

type RedSync struct {
	pools []*redis.Pool
}

func NewRedSync(addrs []net.Addr) *RedSync  {
	red := &RedSync{
		pools: make([]*redis.Pool, len(addrs)),
	}
	for i, addr := range addrs{
		dialTo := addr
		pool := &redis.Pool{
			MaxActive: 1,
			Wait:      true,
			Dial: func() (redis.Conn, error) {
				return redis.Dial("tcp", dialTo.String())
			},
		}

		red.pools[i] = pool
	}

	return red
}


type RedLock struct {
	pools []*redis.Pool
	quorum int // Quorum for the lock, set to len(pools)/2+1
	mutex sync.Mutex
	token string
	key string

}

func (red * RedSync)NewRedLock() *RedLock {
	lens := len(red.pools)
	lock := &RedLock{
		pools: red.pools,
		quorum: lens/2+1,
	}
	return lock
}

//  obtainTtl is extremely smaller than ttl, 50ms-100ms is recommended
func (lock *RedLock) Lock(ctx context.Context, key string, ttl time.Duration, obtainTtl time.Duration, opt *Options) error {
	deadlinectx, cancel := context.WithDeadline(ctx, time.Now().Add(obtainTtl))
	defer cancel()

	var timer *time.Timer
	retry := opt.getRetryStrategy()

	token := make([]byte, 16)
	value, err := makeToken(token)
	if err != nil{
		return err
	}
	value += opt.getMetadata()
	lock.key = key
	lock.token = value


	for{
		n := 0
		for _, pool := range lock.pools{
			conn := pool.Get()
			client := New(conn)
			if ok, err := client.obtain(ctx, key, value, ttl); ok && err ==nil{
				n++
			}
			conn.Close()
		}


		if n >= lock.quorum {
			return nil
		}

		for _, pool := range lock.pools{
			conn := pool.Get()
			client := New(conn)
			client.Release(ctx , key, value)
			conn.Close()
		}

		backoff := retry.NextBackoff()
		if backoff < 1 {
			return ErrNotObtained
		}

		if timer == nil {
			timer = time.NewTimer(backoff)
			//defer timer.Stop()
		} else {
			timer.Reset(backoff)
		}

		select {
		case <-deadlinectx.Done():
			return ErrNotObtained
		case <-timer.C:
		}
	}

}

func (lock *RedLock)Refresh(ctx context.Context, ttl time.Duration)  {
	for _, pool := range lock.pools{
		conn := pool.Get()
		client := New(conn)
		client.Refresh(ctx, lock.key, lock.token, ttl)
		conn.Close()
	}
}

func (lock *RedLock)Release(ctx context.Context)  {
	for _, pool := range lock.pools{
		conn := pool.Get()
		client := New(conn)
		client.Release(ctx, lock.key, lock.token)
		conn.Close()
	}
}
