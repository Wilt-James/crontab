package worker

import (
	"context"
	"crontab/common"
	"go.etcd.io/etcd/clientv3"
	"net"
	"time"
)

// 注册节点到etcd: /cron/workers/IP地址
type Register struct {
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
	localIP string // 本机IP
}

var (
	G_register *Register
)

func getLocalIP() (ipv4 string, err error) {
	// 获取所有网卡
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return
	}
	// 取得第一个非lo的网卡IP
	for _, addr := range addrs {
		if ipNet, isIpNet := addr.(*net.IPNet); isIpNet && !ipNet.IP.IsLoopback() {
			// 跳过IPv6
			if ipNet.IP.To4() != nil {
				ipv4 = ipNet.IP.String()
				return
			}
		}
	}

	err = common.ERR_NO_LOCAL_IP_FOUND
	return
}

// 注册到/cron/workers/IP，并自动续租
func (register *Register) keepOnline() {
	for {
		regKey := common.JOB_WORKER_DIR + register.localIP
		var cancelFunc context.CancelFunc
		leaseGrantResp, err := register.lease.Grant(context.Background(), 10)
		if err != nil {
			time.Sleep(1 * time.Second)
			if cancelFunc != nil {
				cancelFunc()
			}
			return
		}

		keepAliveChan, err := register.lease.KeepAlive(context.Background(), leaseGrantResp.ID)
		if err != nil {
			time.Sleep(1 * time.Second)
			if cancelFunc != nil {
				cancelFunc()
			}
			return
		}

		cancelCtx, cancelFunc := context.WithCancel(context.Background())

		_, err = register.kv.Put(cancelCtx, regKey, "", clientv3.WithLease(leaseGrantResp.ID))
		if err != nil {
			time.Sleep(1 * time.Second)
			if cancelFunc != nil {
				cancelFunc()
			}
			return
		}

		// 处理续租应答
		for {
			select {
			case keepAliveResp := <- keepAliveChan:
				if keepAliveResp == nil { // 续租失败
					time.Sleep(1 * time.Second)
					if cancelFunc != nil {
						cancelFunc()
					}
					return
				}
			}
		}
	}
}

func InitRegister() (err error) {
	config := clientv3.Config{
		Endpoints: G_config.EtcdEndpoints,
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond,
	}

	client, err := clientv3.New(config)
	if err != nil {
		return
	}

	localIp, err := getLocalIP()
	if err != nil {
		return
	}

	kv := clientv3.NewKV(client)
	lease := clientv3.NewLease(client)

	G_register = &Register{
		client:  client,
		kv:      kv,
		lease:   lease,
		localIP: localIp,
	}

	go G_register.keepOnline()
	return
}