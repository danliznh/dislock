package dislock

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var (
	luaRefresh = redis.NewScript(1,`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pexpire", KEYS[1], ARGV[2]) else return 0 end`)
	luaRelease = redis.NewScript(1,`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`)
	luaPTTL    = redis.NewScript(1,`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pttl", KEYS[1]) else return -3 end`)
)

var (
	// ErrNotObtained is returned when a lock cannot be obtained.
	ErrNotObtained = errors.New("dislock: not obtained")

	// ErrLockNotHeld is returned when trying to release an inactive lock.
	ErrLockNotHeld = errors.New("dislock: lock not held")


)

/*// RedisClient is a minimal client interface.
type LockClient interface {
	SetNX(ctx context.Context, unit_key string, value interface{}, expiration time.Duration) error
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) error
	EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) error
	ScriptExists(ctx context.Context, scripts ...string) error
	ScriptLoad(ctx context.Context, script string) error
}*/

func formatMs(dur time.Duration) int64 {
	if dur > 0 && dur < time.Millisecond {
		return 1
	}
	return int64(dur / time.Millisecond)
}

type DisLockClient struct {
	conn redis.Conn
	token []byte
	mutex sync.Mutex
}

func New(conn redis.Conn) *DisLockClient {
	return &DisLockClient{conn: conn}
}

func (c *DisLockClient)randomToken() (string,error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if len(c.token)==0{
		c.token = make([]byte, 16)
	}
	if _, err := io.ReadFull(rand.Reader, c.token); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(c.token), nil
}



func (c *DisLockClient)obtain(ctx context.Context, key,value string, ttl time.Duration) (bool, error)  {

	res, err := redis.String(c.conn.Do("set", key, value ,"px",formatMs(ttl), "nx"))
	if err != nil{
		fmt.Printf("setnx error:%v\n", err)
		return false, nil
	}

	fmt.Printf("setnx result:%s\n", res)
	if res == "OK" {
		return true,nil
	}

	return false, nil

}


// Obtain tries to obtain a new lock using a unit_key with the given TTL.
// May return ErrNotObtained if not successful.
func (c *DisLockClient) Obtain(ctx context.Context, key string, ttl time.Duration, opt *Options) (*Lock, error) {
	// Create a random token
	token, err := c.randomToken()
	if err != nil {
		return nil, err
	}

	value := token + opt.getMetadata()
	retry := opt.getRetryStrategy()

	deadlinectx, cancel := context.WithDeadline(ctx, time.Now().Add(ttl))
	defer cancel()

	var timer *time.Timer
	for {
		ok, err := c.obtain(deadlinectx, key, value, ttl)

		if err != nil {
			return nil, err
		} else if ok {
			lock := &Lock{client: c, key: key, value: value, done: make(chan int,1)}
			safego(lock.autoRefresh, ctx, ttl, opt)
			return lock, nil
		}

		backoff := retry.NextBackoff()
		if backoff < 1 {
			return nil, ErrNotObtained
		}

		if timer == nil {
			timer = time.NewTimer(backoff)
			//defer timer.Stop()
		} else {
			timer.Reset(backoff)
		}

		select {
		case <-deadlinectx.Done():
			return nil, ErrNotObtained
		case <-timer.C:
		}
	}
}

// Lock represents an obtained, distributed lock.
type Lock struct {
	client *DisLockClient
	key    string
	value  string
	done chan int
}

// Obtain is a short-cut for New(...).Obtain(...).
func Obtain(ctx context.Context, conn redis.Conn, key string, ttl time.Duration, opt *Options) (*Lock, error) {
	return New(conn).Obtain(ctx, key, ttl, opt)
}

// Key returns the redis unit_key used by the lock.
func (l *Lock) Key() string {
	return l.key
}

// Token returns the token value set by the lock.
func (l *Lock) Token() string {
	return l.value[:22]
}

// Metadata returns the metadata of the lock.
func (l *Lock) Metadata() string {
	return l.value[22:]
}

// TTL returns the remaining time-to-live. Returns 0 if the lock has expired.
func (l *Lock) TTL(ctx context.Context) (time.Duration, error) {
	res, err := redis.Int(luaPTTL.Do(l.client.conn,[]string{l.key}, l.value))
	if err == redis.ErrNil {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	if res > 0 {
		return time.Duration(res) * time.Millisecond, nil
	}
	return 0, nil
}

// Refresh extends the lock with a new TTL.
// May return ErrNotObtained if refresh is unsuccessful.
func (l *Lock) Refresh(ctx context.Context, ttl time.Duration, opt *Options) error {
	ttlVal := strconv.FormatInt(int64(ttl/time.Millisecond), 10)
	status, err :=  redis.Int(luaRefresh.Do(l.client.conn, l.key, l.value, ttlVal))
	if err != nil {
		return err
	} else if status == int(1) {
		return nil
	}
	return ErrNotObtained
}

func safego(f func(ctx context.Context, ttl time.Duration, opt *Options), ctx context.Context, ttl time.Duration, opt *Options)  {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				println(r)
			}
		}()

		f(ctx, ttl, opt)
	}()
}

func (l *Lock)autoRefresh(ctx context.Context, ttl time.Duration, opt *Options)  {

	//ttlVal := strconv.FormatInt(int64(ttl/time.Millisecond), 10)
	retry := LinearBackoff(ttl/3)


	//deadlinectx, cancel := context.WithDeadline(ctx, time.Now().Add(ttl))
	//defer cancel()

	var timer *time.Timer
	for {
		backoff := retry.NextBackoff()

		fmt.Printf("next backoff:%v\n", backoff)
		if timer == nil {
			timer = time.NewTimer(backoff)
			//defer timer.Stop()
		} else {
			timer.Reset(backoff)
		}
		select {
		case <-l.done:
			//fmt.Printf("the lock released, so exit\n")
			return // exit
		case <-timer.C:
			// continue
		}

		 err := l.Refresh(ctx, ttl, opt)
		 if err != nil && err == ErrNotObtained {
			//fmt.Printf("not obtained exit:%s\n", l.Token())
			break
		}


	}
}

// Release manually releases the lock.
// May return ErrLockNotHeld.
func (l *Lock) Release(ctx context.Context) error {
	//fmt.Printf("%p", l.client.conn)
	res, err :=  redis.Int(luaRelease.Do(l.client.conn,l.key,l.value))
	if err == redis.ErrNil {
		l.done <- 1
		return ErrLockNotHeld
	} else if err != nil {
		return err
	}

	l.done <-1
	if  res != 1 {
		return ErrLockNotHeld
	}
	return nil
}

func (l *Lock) releaseForRedLock(ctx context.Context) error{
	err := l.Release(ctx)
	l.client.conn.Close()
	return err
}


// --------------------------------------------------------------------

// Options describe the options for the lock
type Options struct {
	// RetryStrategy allows to customise the lock retry strategy.
	// Default: do not retry
	RetryStrategy RetryStrategy

	// Metadata string is appended to the lock token.
	Metadata string
}

func (o *Options) getMetadata() string {
	if o != nil {
		return o.Metadata
	}
	return ""
}

func (o *Options) getRetryStrategy() RetryStrategy {
	if o != nil && o.RetryStrategy != nil {
		return o.RetryStrategy
	}
	return NoRetry()
}

// --------------------------------------------------------------------

// RetryStrategy allows to customise the lock retry strategy.
type RetryStrategy interface {
	// NextBackoff returns the next backoff duration.
	NextBackoff() time.Duration
}

type linearBackoff time.Duration

// LinearBackoff allows retries regularly with customized intervals
func LinearBackoff(backoff time.Duration) RetryStrategy {
	return linearBackoff(backoff)
}

// NoRetry acquire the lock only once.
func NoRetry() RetryStrategy {
	return linearBackoff(0)
}

func (r linearBackoff) NextBackoff() time.Duration {
	return time.Duration(r)
}

type limitedRetry struct {
	s   RetryStrategy
	cnt int64
	max int64
}

// LimitRetry limits the number of retries to max attempts.
func LimitRetry(s RetryStrategy, max int) RetryStrategy {
	return &limitedRetry{s: s, max: int64(max)}
}

func (r *limitedRetry) NextBackoff() time.Duration {
	if atomic.LoadInt64(&r.cnt) >= r.max {
		return 0
	}
	atomic.AddInt64(&r.cnt, 1)
	return r.s.NextBackoff()
}

type exponentialBackoff struct {
	cnt uint64

	min, max time.Duration
}

// ExponentialBackoff strategy is an optimization strategy with a retry time of 2**n milliseconds (n means number of times).
// You can set a minimum and maximum value, the recommended minimum value is not less than 16ms.
func ExponentialBackoff(min, max time.Duration) RetryStrategy {
	return &exponentialBackoff{min: min, max: max}
}

func (r *exponentialBackoff) NextBackoff() time.Duration {
	cnt := atomic.AddUint64(&r.cnt, 1)

	ms := 2 << 25
	if cnt < 25 {
		ms = 2 << cnt
	}

	if d := time.Duration(ms) * time.Millisecond; d < r.min {
		return r.min
	} else if r.max != 0 && d > r.max {
		return r.max
	} else {
		return d
	}
}



