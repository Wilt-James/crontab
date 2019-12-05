package worker

import (
	"context"
	"crontab/common"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"time"
)

// 任务管理器
type JobMgr struct {
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
	watcher clientv3.Watcher
}

var (
	G_jobMgr *JobMgr
)

// 监听任务变化
func (jobMgr *JobMgr) watchJobs() (err error) {
	getResp, err := jobMgr.kv.Get(context.Background(), common.JOB_SAVE_DIR, clientv3.WithPrefix())
	if err != nil {
		return
	}

	for _, kvpair := range getResp.Kvs {
		job, err := common.UnpackJob(kvpair.Value)
		if err != nil {
			continue
		}
		jobEvent := common.BuildJobEvent(common.JOB_EVENT_SAVE, job)

		G_scheduler.PushJobEvent(jobEvent)

	}

	go func() {
		watchStartRevision := getResp.Header.Revision + 1
		watchChan := jobMgr.watcher.Watch(context.Background(), common.JOB_SAVE_DIR, clientv3.WithRev(watchStartRevision), clientv3.WithPrefix())
		for watchResp := range watchChan {
			for _, watchEvent := range watchResp.Events {
				var jobEvent *common.JobEvent
				switch watchEvent.Type {
				case mvccpb.PUT: // 任务保存事件
					job, err := common.UnpackJob(watchEvent.Kv.Value)
					if err != nil {
						continue
					}
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, job)
				case mvccpb.DELETE: // 任务删除事件
					jobName := common.ExtractJobName(string(watchEvent.Kv.Key))
					job := &common.Job{Name: jobName}
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_DELETE, job)
				}

				G_scheduler.PushJobEvent(jobEvent)
			}
		}
		return
	}()
	return
}

// 监听强杀任务通知
func (jobMgr *JobMgr) watchKiller() {
	go func() { //监听协程
		// 监听/cron/killer/目录的变化
		watchChan := jobMgr.watcher.Watch(context.Background(), common.JOB_KILLER_DIR, clientv3.WithPrefix())
		// 处理监听事件
		for watchResp := range watchChan {
			for _, watchEvent := range watchResp.Events {
				var jobEvent *common.JobEvent
				switch watchEvent.Type {
				case mvccpb.PUT: // 杀死任务事件
					jobName := common.ExtractKillerName(string(watchEvent.Kv.Key))
					job := &common.Job{Name: jobName}
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_KILL, job)
					// 事件推给scheduler
					G_scheduler.PushJobEvent(jobEvent)
				case mvccpb.DELETE: // killer标记过期，被自动删除
				}
			}
		}
		return
	}()
	return
}

// 初始化管理器
func InitJobMgr() (err error) {
	config := clientv3.Config{
		Endpoints:            G_config.EtcdEndpoints,
		DialTimeout:          time.Duration(G_config.EtcdDialTimeout) * time.Millisecond,
	}

	client, err := clientv3.New(config)
	if err != nil {
		return
	}

	kv := clientv3.NewKV(client)
	lease := clientv3.NewLease(client)
	watcher := clientv3.NewWatcher(client)

	G_jobMgr = &JobMgr{
		client:		client,
		kv:			kv,
		lease:		lease,
		watcher:	watcher,
	}

	G_jobMgr.watchJobs()
	G_jobMgr.watchKiller()

	return
}

// 创建任务执行锁
func (jobMgr *JobMgr) CreateJobLock(jobName string) (jobLock *JobLock) {
	jobLock = InitJobLock(jobName, jobMgr.kv, jobMgr.lease)
	return
}