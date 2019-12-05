package master

import (
	"context"
	"crontab/common"
	"go.etcd.io/etcd/clientv3"
	"time"
)

// /cron/workers/
type WorkerMgr struct {
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
}

var (
	G_workerMgr *WorkerMgr
)

// 获取在线worker列表
func (workerMgr *WorkerMgr) ListWorkers() (workerArr []string, err error) {
	workerArr = make([]string, 0)
	getResp, err := workerMgr.kv.Get(context.Background(), common.JOB_WORKER_DIR, clientv3.WithPrefix())
	if err != nil {
		return
	}

	// 解析每个节点的IP
	for _, kv := range getResp.Kvs {
		// kv.Key : /cron/workers/192.168.1.1
		workerIP := common.ExtractWorkerIP(string(kv.Key))
		workerArr = append(workerArr, workerIP)
	}
	return
}

func InitWorkerMgr() (err error) {
	config := clientv3.Config{
		Endpoints: G_config.EtcdEndpoints,
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond,
	}

	client, err := clientv3.New(config)
	if err != nil {
		return
	}

	kv := clientv3.NewKV(client)
	lease := clientv3.NewLease(client)

	G_workerMgr = &WorkerMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}
	return
}
