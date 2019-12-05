package master

import (
	"context"
	"crontab/common"
	"encoding/json"
	"go.etcd.io/etcd/clientv3"
	"time"
)

// 任务管理器
type JobMgr struct {
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
}

var (
	G_jobMgr *JobMgr
)

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

	G_jobMgr = &JobMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}

	return

}

// 保存任务
func (jobMgr *JobMgr) SaveJob(job *common.Job) (oldJob *common.Job, err error) {
	jobKey := common.JOB_SAVE_DIR + job.Name
	jobValue, err := json.Marshal(job)
	if err != nil {
		return
	}

	putResp, err := jobMgr.kv.Put(context.Background(), jobKey, string(jobValue), clientv3.WithPrevKV())
	if err != nil {
		return
	}

	var oldJobObj common.Job
	if putResp.PrevKv != nil {
		errJson := json.Unmarshal(putResp.PrevKv.Value, &oldJobObj)
		if errJson != nil {
			return
		}
		oldJob = &oldJobObj
	}

	return
}

// 删除任务
func (jobMgr *JobMgr) DeleteJob(name string) (oldJob *common.Job, err error) {
	jobKey := common.JOB_SAVE_DIR + name

	delResp, err := jobMgr.kv.Delete(context.Background(), jobKey, clientv3.WithPrevKV())
	if err != nil {
		return
	}

	var oldJobObj common.Job
	if len(delResp.PrevKvs) != 0 {
		errJson := json.Unmarshal(delResp.PrevKvs[0].Value, &oldJobObj)
		if errJson != nil {
			return
		}
		oldJob = &oldJobObj
	}

	return
}

// 列举任务
func (jobMgr *JobMgr) ListJobs() (jobList []*common.Job, err error) {
	dirKey := common.JOB_SAVE_DIR

	getResp, err := jobMgr.kv.Get(context.Background(), dirKey, clientv3.WithPrefix())
	if err != nil {
		return
	}

	jobList = make([]*common.Job, 0)

	for _, kvPair := range getResp.Kvs {
		job := &common.Job{}
		errJson := json.Unmarshal(kvPair.Value, job)
		if errJson != nil {
			continue
		}
		jobList = append(jobList, job)
	}

	return
}

// 杀死任务
func (jobMgr *JobMgr) KillJob(name string) (err error) {
	killerKey := common.JOB_KILLER_DIR + name

	leaseGrantResp, err := jobMgr.lease.Grant(context.Background(), 1)
	if err != nil {
		return
	}

	leaseId := leaseGrantResp.ID

	_, err = jobMgr.kv.Put(context.Background(), killerKey, "", clientv3.WithLease(leaseId))
	if err != nil {
		return
	}

	return
}