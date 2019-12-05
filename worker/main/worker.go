package main

import (
	"crontab/worker"
	"flag"
	"fmt"
	"time"
)

var (
	confFile string
)

// 解析命令行参数
func initArgs() {
	//worker -config ./worker.json
	flag.StringVar(&confFile, "config", "D:\\Go project\\src\\crontab\\worker\\main\\worker.json", "指定worker.json")
	flag.Parse()
}

func main() {
	var err error

	initArgs()

	err = worker.InitConfig(confFile)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = worker.InitRegister()
	if err != nil {
		fmt.Println(err)
		return
	}

	err = worker.InitLogSink()
	if err != nil {
		fmt.Println(err)
		return
	}

	err = worker.InitExecutor()
	if err != nil {
		fmt.Println(err)
		return
	}

	err = worker.InitScheduler()
	if err != nil {
		fmt.Println(err)
		return
	}

	err = worker.InitJobMgr()
	if err != nil {
		fmt.Println(err)
		return
	}

	for {
		time.Sleep(1)
	}
	return
}