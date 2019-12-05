package worker

import (
	"crontab/common"
	"math/rand"
	"os/exec"
	"time"
)

// 任务执行器
type Executor struct {

}

var (
	G_executor *Executor
)

// 执行一个任务
func (executor *Executor) ExecuteJob(info *common.JobExecuteInfo) {
	go func() {
		result := &common.JobExecuteResult{
			ExecuteInfo: info,
			Output:      make([]byte, 0),
		}

		jobLock := G_jobMgr.CreateJobLock(info.Job.Name)

		result.StartTime = time.Now()

		// 机器时间不精准，导致分布式集群总是由某台机器抢到任务，所以随机睡眠0到1秒
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
		err := jobLock.TryLock()
		defer jobLock.Unlock()

		if err != nil {
			result.Err = err
			result.EndTime = time.Now()
		} else {
			result.StartTime = time.Now()
			cmd := exec.CommandContext(info.CancelCtx, "D:\\cygwin64\\bin\\bash.exe", "-c", info.Job.Command)
			output, err := cmd.CombinedOutput()
			result.EndTime = time.Now()
			result.Output = output
			result.Err = err
		}

		G_scheduler.PushJobResult(result)
	}()
}

// 初始化执行器
func InitExecutor() (err error) {
	G_executor = &Executor{}
	return
}