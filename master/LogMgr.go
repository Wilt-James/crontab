package master

import (
	"context"
	"crontab/common"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/clientopt"
	"github.com/mongodb/mongo-go-driver/mongo/findopt"
	"time"
)

// mongodb日志管理
type LogMgr struct {
	client *mongo.Client
	logCollection *mongo.Collection
}

var (
	G_logMgr *LogMgr
)

func InitLogMgr() (err error) {
	client, err := mongo.Connect(
		context.Background(),
		G_config.MongodbUri,
		clientopt.ConnectTimeout(time.Duration(G_config.MongodbConnectTimeout) * time.Millisecond))
	if err != nil {
		return
	}

	G_logMgr = &LogMgr{
		client:        client,
		logCollection: client.Database("cron").Collection("log"),
	}
	return
}

// 查看任务日志
func (logMgr *LogMgr) ListLog(name string, skip int, limit int) (logArr []*common.JobLog, err error) {
	logArr = make([]*common.JobLog, 0)
	filter := &common.JobLogFilter{JobName: name}
	logSort := &common.SortLogByStartTime{SortOrder: -1}

	cursor, err := logMgr.logCollection.Find(context.Background(), filter, findopt.Sort(logSort), findopt.Skip(int64(skip)), findopt.Limit(int64(limit)))
	if err != nil {
		return
	}
	defer cursor.Close(context.Background())

	for cursor.Next(context.Background()) {
		jobLog := &common.JobLog{}
		err = cursor.Decode(jobLog)
		if err != nil {
			continue // 有日志不合法
		}
		logArr = append(logArr, jobLog)
	}
	return
}