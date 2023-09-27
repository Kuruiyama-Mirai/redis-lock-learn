package redis_lock_learn

import "time"

const (
	// 默认连接池超过 10 s 释放连接
	DefaultIdleTimeoutSeconds = 10
	// 默认最大激活连接数
	DefaultMaxActive = 100
	// 默认最大空闲连接数
	DefaultMaxIdle = 20

	// 默认的分布式锁过期时间
	DefaultLockExpireSeconds = 30
	// 看门狗工作时间间隙
	WatchDogWorkStepSeconds = 10
)

// 客户端配置项
type ClientOptions struct {
	maxIdle            int
	idleTimeoutSeconds int
	maxActive          int
	wait               bool
	// 必填参数
	network  string
	address  string
	password string
}

type ClientOption func(c *ClientOptions)

func WithMaxIdle(maxIdle int) ClientOption {
	return func(c *ClientOptions) {
		c.maxIdle = maxIdle
	}
}

func WithIdleTimeoutSeconds(idleTimeoutSeconds int) ClientOption {
	return func(c *ClientOptions) {
		c.idleTimeoutSeconds = idleTimeoutSeconds
	}
}

func WithMaxActive(maxActive int) ClientOption {
	return func(c *ClientOptions) {
		c.maxActive = maxActive
	}
}

func WithWaitMode() ClientOption {
	return func(c *ClientOptions) {
		c.wait = true
	}
}

// 默认的自动配置客户端
func repairClient(c *ClientOptions) {
	if c.maxIdle < 0 {
		c.maxIdle = DefaultMaxIdle
	}

	if c.idleTimeoutSeconds < 0 {
		c.idleTimeoutSeconds = DefaultIdleTimeoutSeconds
	}

	if c.maxActive < 0 {
		c.maxActive = DefaultMaxActive
	}
}

// 锁配置项
// 看门狗机制：在锁的持有方执行业务逻辑处理的过程中时，需要异步启动一个看门狗守护协程，持续为分布式锁的过期阈值进行延期操作
type LockOptions struct {
	//是否使用阻塞模式
	isBlock             bool
	blockWaitingSeconds int64
	expireSeconds       int64
	watchDogMode        bool
}

type LockOption func(*LockOptions)

func WithBlock() LockOption {
	return func(o *LockOptions) {
		o.isBlock = true
	}
}

func WithBlockWaitingSeconds(waitingSeconds int64) LockOption {
	return func(o *LockOptions) {
		o.blockWaitingSeconds = waitingSeconds
	}
}

func WithExpireSeconds(expireSeconds int64) LockOption {
	return func(o *LockOptions) {
		o.expireSeconds = expireSeconds
	}
}

func repairLock(o *LockOptions) {
	if o.isBlock && o.blockWaitingSeconds <= 0 {
		// 默认阻塞等待时间上限为 5 秒
		o.blockWaitingSeconds = 5
	}

	// 倘若未设置分布式锁的过期时间，则会启动 watchdog
	if o.expireSeconds > 0 {
		return
	}

	// 用户未显式指定锁的过期时间，则此时会启动看门狗
	o.expireSeconds = DefaultLockExpireSeconds
	o.watchDogMode = true
}

//红锁：通过增加锁的数量并基于多数派准则
type RedLockOptions struct {
	//单节点时间
	singleNodesTimeout time.Duration
	expireDuration     time.Duration
}

type RedLockOption func(*RedLockOptions)

func WithSingleNodesTimeout(singleNodesTimeout time.Duration) RedLockOption {
	return func(o *RedLockOptions) {
		o.singleNodesTimeout = singleNodesTimeout
	}
}

func WithRedLockExpireDuration(expireDuration time.Duration) RedLockOption {
	return func(o *RedLockOptions) {
		o.expireDuration = expireDuration
	}
}

//默认红锁配置
func repairRedLock(o *RedLockOptions) {
	if o.singleNodesTimeout <= 0 {
		o.singleNodesTimeout = DefaultSingleLockTimeout
	}
}

//单节点
type SingleNodeConf struct {
	Network  string
	Address  string
	Password string
	Opts     []ClientOption
}
