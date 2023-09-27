package redis_lock_learn

import (
	"context"
	"errors"
	"time"
)

// 红锁中每个节点默认的处理超时时间为 50 ms
const DefaultSingleLockTimeout = 50 * time.Millisecond

type RedLock struct {
	locks []*RedisLock
	RedLockOptions
}

func NewRedLock(key string, confs []*SingleNodeConf, opts ...RedLockOption) (*RedLock, error) {
	// 3 个节点以上，红锁才有意义
	if len(confs) < 3 {
		return nil, errors.New("can not use redLock less than 3 nodes")
	}

	r := RedLock{}
	for _, opt := range opts {
		opt(&r.RedLockOptions)
	}

	repairRedLock(&r.RedLockOptions)
	if r.expireDuration > 0 && time.Duration(len(confs))*r.singleNodesTimeout*10 > r.expireDuration {
		// 要求所有节点累计的超时阈值要小于分布式锁过期时间的十分之一
		return nil, errors.New("expire thresholds of single node is too long")
	}

	r.locks = make([]*RedisLock, 0, len(confs))
	for _, conf := range confs {
		client := NewClient(conf.Network, conf.Address, conf.Password, conf.Opts...)
		r.locks = append(r.locks, NewRedisLock(key, client, WithExpireSeconds(int64(r.expireDuration.Seconds()))))
	}

	return &r, nil
}

/*
遍历对所有的 redis 锁节点，分别执行加锁操作

• 对每笔 redis 锁节点的交互请求进行时间限制，保证控制在 singleNodesTimeout 之内

• 对 singleNodesTimeout 请求耗时内成功完成的加锁请求数进行记录

• 遍历执行完所有 redis 节点的加锁操作后，倘若成功加锁请求数量达到 redis 锁节点总数的一半以上，则视为红锁加锁成功

• 倘若 redis 节点锁加锁成功数量未达到多数，则红锁加锁失败，此时会调用红锁的解锁操作，尝试对所有的 redis 锁节点执行一次解锁操作
*/
func (r *RedLock) Lock(ctx context.Context) error {
	var successCnt int
	for _, lock := range r.locks {
		startTime := time.Now()
		err := lock.Lock(ctx)
		cost := time.Since(startTime)
		if err == nil && cost <= r.singleNodesTimeout {
			successCnt++
		}
	}

	if successCnt < len(r.locks)>>1+1 {
		return errors.New("lock failed")
	}
	return nil
}

// 遍历所有的 redis 锁节点，对所有节点广播解锁，依次执行解锁操作
func (r *RedLock) Unlock(ctx context.Context) error {
	var err error
	for _, lock := range r.locks {
		if _err := lock.Unlock(ctx); _err != nil {
			if err == nil {
				err = _err
			}
		}
	}
	return err
}
