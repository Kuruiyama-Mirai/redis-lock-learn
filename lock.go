package redis_lock_learn

import (
	"context"
	"errors"
	"fmt"
	"redis-lock-learn/utils"
	"sync/atomic"
	"time"

	"github.com/gomodule/redigo/redis"
)

/*
所谓分布式锁，应当基本如下几项核心性质：

• 独占性：对于同一把锁，在同一时刻只能被一个取锁方占有，这是锁最基础的一项特征

• 健壮性：即不能产生死锁（dead lock）. 假如某个占有锁的使用方因为宕机而无法主动执行解锁动作，锁也应该能够被正常传承下去，被其他使用方所延续使用

• 对称性：加锁和解锁的使用方必须为同一身份. 不允许非法释放他人持有的分布式锁

• 高可用：当提供分布式锁服务的基础组件中存在少量节点发生故障时，应该不能影响到分布式锁服务的稳定性
*/

/*
RedisLock 是对应于分布式锁的类型. 其中内置了如下几个字段：
1 配置类 LockOptions
2 分布式锁的唯一标识键 key
3 分布式锁使用方的身份标识 token
4 分布式锁与 redis 交互的客户端 client
5 runningDog 和 stopDog 两个字段和看门狗模式相关
*/
const RedisLockKeyPrefix = "REDIS_LOCK_PREFIX_"

var ErrLockAcquiredByOthers = errors.New("lock is acquired by others")

// ErrNil indicates that a reply value is nil.
var ErrNil = redis.ErrNil

// 判断是否是已经存在的错误
func IsRetryableErr(err error) bool {
	return errors.Is(err, ErrLockAcquiredByOthers)
}

// 基于 redis 实现的分布式锁，不可重入，但保证了对称性
type RedisLock struct {
	LockOptions
	key    string
	token  string
	client LockClient
	// 看门狗运作标识 检验是否有看门狗正在运行
	runningDog int32
	// 停止看门狗 A CancelFunc tells an operation to abandon its work.
	stopDog context.CancelFunc
}

func NewRedisLock(key string, client LockClient, opts ...LockOption) *RedisLock {
	r := RedisLock{
		key:    key,
		token:  utils.GetProcessAndGoroutineIDStr(),
		client: client,
	}

	for _, opt := range opts {
		opt(&r.LockOptions)
	}
	repairLock(&r.LockOptions)
	return &r
}

// Lock加锁
func (r *RedisLock) Lock(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
			return
		}
		// 加锁成功的情况下，会步入的启动看门狗
		// 关于该锁本身是不可重入的，所以不会出现同一把锁下看门狗重复启动的情况
		r.watchDog(ctx)
	}()

	// 不管是不是阻塞模式，都要先获取一次锁
	err = r.tryLock(ctx)
	if err == nil {
		return nil
	}

	// 非阻塞模式加锁失败直接返回错误
	if !r.isBlock {
		return err
	}

	// 判断错误是否可以允许重试，不可允许的类型则直接返回错误
	if !IsRetryableErr(err) {
		return err
	}

	// 基于阻塞模式持续轮询取锁
	err = r.blockingLock(ctx)
	return
}

/*
Unlock 解锁. 基于 lua 脚本实现操作原子性.
保证原子化地执行两个步骤：
• 校验锁是否属于自己.（拿当前锁的 token 和锁数据的 val 进行对比）
• 如果锁是属于自己的，则调用 redis del 指令删除锁数据
*/
func (r *RedisLock) Unlock(ctx context.Context) error {
	defer func() {
		//停止看门狗
		if r.stopDog != nil {
			r.stopDog()
		}
	}()

	keysAndArgs := []interface{}{r.getLockKey(), r.token}
	reply, err := r.client.Eval(ctx, LuaCheckAndDeleteDistributionLock, 1, keysAndArgs)
	if err != nil {
		return err
	}

	if ret, _ := reply.(int64); ret != 1 {
		return errors.New("can not unlock without ownership of lock")
	}

	return nil
}

// 在非阻塞模式下，加锁流程会尝试执行一次 setNEX 动作，倘若发现锁数据已经存在，说明锁已经被他人持有，此时会直接返回错误.
func (r *RedisLock) tryLock(ctx context.Context) error {
	// 首先查询锁是否属于自己
	reply, err := r.client.SetNEX(ctx, r.getLockKey(), r.token, r.expireSeconds)
	if err != nil {
		return err
	}
	if reply != 1 {
		return fmt.Errorf("reply: %d, err: %w", reply, ErrLockAcquiredByOthers)
	}

	return nil
}

/*
在阻塞模式下，会创建一个执行间隔为 50 ms 的 time ticker，
在 time ticker 的驱动下会轮询执行 tryLock 操作尝试取锁，
直到出现下述四种情况之一时，流程才会结束：

• 成功取到锁

• 上下文 context 被终止

• 阻塞模式等锁的超时阈值达到了

• 取锁时遇到了预期之外的错误
*/
func (r *RedisLock) blockingLock(ctx context.Context) error {
	// 阻塞模式等待锁时间上限
	timeoutCh := time.After(time.Duration(r.blockWaitingSeconds) * time.Second)
	// 轮询 ticker，每隔 50 ms 尝试取锁一次
	ticker := time.NewTicker(time.Duration(50) * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		select {
		//ctx终止
		case <-ctx.Done():
			return fmt.Errorf("lock failed, ctx timeout, err: %w", ctx.Err())
		//阻塞等锁达到上限时间
		case <-timeoutCh:
			return fmt.Errorf("block waiting time out, err: %w", ErrLockAcquiredByOthers)
		//默认情况放行
		default:
		}

		//尝试取锁
		err := r.tryLock(ctx)
		if err == nil {
			// 加锁成功，返回结果
			return nil
		}
		// 不可重试类型的错误，直接返回
		if !IsRetryableErr(err) {
			return err
		}
	}
	return nil
}

// 启动看门狗
func (r *RedisLock) watchDog(ctx context.Context) {
	// 1. 非看门狗模式，不处理
	if !r.watchDogMode {
		return
	}

	// 2. 确保之前启动的看门狗已经正常回收
	//开启一轮 cas 自旋操作，确保基于 cas 操作将 runningDog 标识的值由 0 改为 1 时，
	//流程才会向下执行. 这段自旋流程的目的是为了避免看门狗的重复运行
	for !atomic.CompareAndSwapInt32(&r.runningDog, 0, 1) {

	}

	// 3. 启动看门狗
	//创建出一个子 context 用于协调看门狗协程的生命周期，
	//同时将子 context 的 cancel 函数注入到 RedisLock.stopLock 当中，作为后续关闭看门狗协程的控制器
	ctx, r.stopDog = context.WithCancel(ctx)
	go func() {
		defer func() {
			//将0原子化操作赋给看门狗标识位
			atomic.StoreInt32(&r.runningDog, 0)
		}()
		//遵循用户定义的时间节奏，持续地执行对分布式锁的延期操作.
		r.runWatchDog(ctx)
	}()
}

func (r *RedisLock) runWatchDog(ctx context.Context) {
	ticker := time.NewTicker(WatchDogWorkStepSeconds * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// 看门狗负责在用户未显式解锁时，持续为分布式锁进行续期
		// 通过 lua 脚本，延期之前会确保保证锁仍然属于自己
		// 为避免因为网络延迟而导致锁被提前释放的问题，watch dog 续约时需要把锁的过期时长额外增加 5 s
		_ = r.DelayExpire(ctx, WatchDogWorkStepSeconds+5)
	}
}

/*
延期锁 负责延长看门狗时间
• 校验锁是否属于自己.（拿当前锁的 token 和锁数据的 val 进行对比）
• 如果锁是属于自己的，则调用 redis 的 expire 指令进行锁数据的延期操作
*/
// 更新锁的过期时间，基于 lua 脚本实现操作原子性
func (r *RedisLock) DelayExpire(ctx context.Context, expireSeconds int64) error {
	keysAndArgs := []interface{}{r.getLockKey(), r.token, expireSeconds}
	reply, err := r.client.Eval(ctx, LuaCheckAndExpireDistributionLock, 1, keysAndArgs)
	if err != nil {
		return err
	}

	if ret, _ := reply.(int64); ret != 1 {
		return errors.New("can not expire lock without ownership of lock")
	}

	return nil
}

// 拼接一个唯一的Key
func (r *RedisLock) getLockKey() string {
	return RedisLockKeyPrefix + r.key
}
