package delayqueue

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/panjf2000/ants/v2"
)

type DelayQueue struct {
	client *redis.Client
	key    string
	f      func(error, string)
	gpool  *ants.Pool
}

func New(client *redis.Client, key string, f func(error, string)) *DelayQueue {
	return &DelayQueue{
		client: client,
		key:    key,
		f:      f,
	}
}
func NewWithGPool(client *redis.Client, key string, f func(error, string), gpool *ants.Pool) *DelayQueue {
	return &DelayQueue{
		client: client,
		key:    key,
		f:      f,
		gpool:  gpool,
	}
}
func (d *DelayQueue) Push(ctx context.Context, value string, delaySecond, maxTTL int64) error {
	expireSecond := time.Now().Unix() + delaySecond

	timePiece := fmt.Sprintf("dq:%s:%d", d.key, expireSecond)
	z := redis.Z{Score: float64(expireSecond), Member: timePiece}
	v, err := d.client.ZAddNX(ctx, d.key, &z).Result()
	if err != nil {
		return err
	}
	_, err = d.client.RPush(ctx, timePiece, value).Result()
	if err != nil {
		return err
	}

	if v > 0 {
		d.client.Expire(ctx, timePiece, time.Second*time.Duration(maxTTL+delaySecond))
	}
	return err
}
func (d *DelayQueue) Run() {
	go func() {
		ctx := context.Background()
		ticker := time.NewTicker(time.Second)
		for {
			t := <-ticker.C
			now := t.Unix()
			opt := redis.ZRangeBy{Min: "0", Max: strconv.FormatInt(now, 10), Count: 1}
			rsp, err := d.client.ZRangeByScore(ctx, d.key, &opt).Result()
			if err != nil {
				d.f(err, "")
				continue
			}
			if len(rsp) == 0 {
				continue
			}
			unixStr := rsp[0]
			datas, err := d.client.LRange(ctx, unixStr, 0, -1).Result()
			if err != nil {
				d.f(err, "")
				break
			}

			ff := func() {
				for _, v := range datas {
					d.f(nil, v)
				}
			}
			if d.gpool != nil {
				err = d.gpool.Submit(ff)
				if err != nil {
					d.f(err, "")
				}
			} else {
				ff()
			}
			d.client.ZRem(ctx, d.key, unixStr)
			d.client.Del(ctx, unixStr)
		}
	}()
}
