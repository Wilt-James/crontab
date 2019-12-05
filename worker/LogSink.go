package worker

import (
	"context"
	"crontab/common"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/clientopt"
	"time"
)

// mongodb存储日志
type LogSink struct {
	client *mongo.Client
	logCollection *mongo.Collection
	logChan chan *common.JobLog
	autoCommitChan chan *common.LogBatch
}

var (
	G_logSink *LogSink
)

// 批量写入日志
func (logSink *LogSink) saveLogs(batch *common.LogBatch) {
	logSink.logCollection.InsertMany(context.Background(), batch.Logs)
}

// 日志存储协程
func (logSink *LogSink) writeLoop() {
	var logBatch *common.LogBatch // 当前的批次
	var commitTimer *time.Timer
	for {
		select {
		case log := <- logSink.logChan:
			if logBatch == nil {
				logBatch = &common.LogBatch{}
				// 让这个批次超时自动提交（给1秒的时间）
				// 如果不设置闭包，当前logBatch可能会继续执行接下去的逻辑
				commitTimer = time.AfterFunc(time.Duration(G_config.JobLogCommitTimeout) * time.Millisecond,
					func(batch *common.LogBatch) func() {
						return func() {
							logSink.autoCommitChan <- batch
						}
					}(logBatch))
			}
			logBatch.Logs = append(logBatch.Logs, log)
			if len(logBatch.Logs) >= G_config.JobLogBatchSize {
				logSink.saveLogs(logBatch)
				logBatch = nil
				commitTimer.Stop() // 取消定时器
			}
		case timeoutBatch := <- logSink.autoCommitChan:
			// 判断过期批次是否仍旧是当前的批次
			if timeoutBatch != logBatch {
				continue // 跳过已经被提交的批次
			}
			logSink.saveLogs(timeoutBatch)
			logBatch = nil
		}
	}
}

func InitLogSink() (err error) {
	client, err := mongo.Connect(
		context.Background(),
		G_config.MongodbUri,
		clientopt.ConnectTimeout(time.Duration(G_config.MongodbConnectTimeout) * time.Millisecond))
	if err != nil {
		return
	}

	G_logSink = &LogSink{
		client:         client,
		logCollection:  client.Database("cron").Collection("log"),
		logChan:        make(chan *common.JobLog, 1000),
		autoCommitChan: make(chan *common.LogBatch, 1000),
	}

	go G_logSink.writeLoop()
	return
}

// 发送日志
func (logSink *LogSink) Append(jobLog *common.JobLog) {
	select {
	case logSink.logChan <- jobLog:
	default:
		// 队列满了就丢弃
	}
}
