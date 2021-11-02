package dislock

import (
	"context"
	"github.com/gomodule/redigo/redis"
	"net"
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

	locked []*Lock
	quorum int // Quorum for the lock, set to len(pools)/2+1

}

func (red * RedSync)NewRedLock() *RedLock {

	lens := len(red.pools)
	lock := &RedLock{
		pools: red.pools,
		locked: make([]*Lock, lens),
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
	for{
		n := 0
		locked := make([]*Lock, len(lock.pools))
		for _ , pool := range lock.pools{
			conn := pool.Get()
			disLock, err := Obtain(ctx, conn, key, ttl, opt)
			if err != nil{
				continue
			}
			locked[n] = disLock
			n++
		}

		if n >= lock.quorum {
			// there must be one thread that can obtained the number of locks more than half, so there is no lock to write the lock.locked
			lock.locked = locked
			return nil
		}

		for _, l := range locked{
			l.releaseForRedLock(ctx)
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

func (lock *RedLock)Refresh(ctx context.Context, ttl time.Duration, opt *Options)  {
	for _, l := range lock.locked{
		l.Refresh(ctx, ttl, opt)
	}
}

func (lock *RedLock)Release(ctx context.Context)  {
	if lock.locked == nil{
		return
	}
	for _, l := range lock.locked{
		l.Release(ctx)
	}
}
