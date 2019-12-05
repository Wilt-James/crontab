package worker

import (
	"context"
	"crontab/common"
	"go.etcd.io/etcd/clientv3"
)

// 分布式锁（TXN事务）
type JobLock struct {
	// etcd客户端
	kv clientv3.KV
	lease clientv3.Lease

	jobName string // 任务名
	cancelFunc context.CancelFunc // 用于终止自动续租
	leaseId clientv3.LeaseID // 租约ID
	isLocked bool // 是否上锁成功
}

// 初始化一把锁
func InitJobLock(jobName string, kv clientv3.KV, lease clientv3.Lease) (jobLock *JobLock) {
	jobLock = &JobLock{
		kv: kv,
		lease: lease,
		jobName: jobName,
	}
	return
}

// 尝试上锁
func (jobLock *JobLock) TryLock() (err error) {

	// 1. 创建租约（5秒）
	leaseGrantResp, err := jobLock.lease.Grant(context.Background(), 5)
	if err != nil {
		return
	}
	cancelCtx, cancelFunc := context.WithCancel(context.Background())
	leaseId := leaseGrantResp.ID

	// 2. 自动续租
	keepRespChan, err := jobLock.lease.KeepAlive(cancelCtx, leaseId)
	if err != nil {
		cancelFunc()
		jobLock.lease.Revoke(context.Background(), leaseId)
		return
	}

	// 3. 处理续租应答的协程
	go func() {
		for {
			select {
			case keepResp := <- keepRespChan:
				if keepResp == nil {
					return
				}
			}
		}
		return
	}()

	// 4. 创建事务txn
	txn := jobLock.kv.Txn((context.Background()))
	lockKey := common.JOB_LOCK_DIR + jobLock.jobName

	// 5. 事务抢锁
	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).
		Then(clientv3.OpPut(lockKey, "", clientv3.WithLease(leaseId))).
		Else(clientv3.OpGet(lockKey))
	txnResp, err := txn.Commit()
	if err != nil {
		cancelFunc()
		jobLock.lease.Revoke(context.Background(), leaseId)
		return
	}

	// 6. 成功返回，失败释放租约
	if !txnResp.Succeeded {
		err = common.ERR_LOCK_ALREADY_REQUIRED
		cancelFunc()
		jobLock.lease.Revoke(context.Background(), leaseId)
		return
	}

	// 抢锁成功
	jobLock.leaseId = leaseId
	jobLock.cancelFunc = cancelFunc
	jobLock.isLocked = true
	return
}

// 释放锁
func (jobLock *JobLock) Unlock() {
	if jobLock.isLocked {
		jobLock.cancelFunc()
		jobLock.lease.Revoke(context.Background(), jobLock.leaseId)
	}
	return
}
