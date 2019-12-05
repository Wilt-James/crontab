package main

import (
	"crontab/master"
	"flag"
	"fmt"
	"time"
)

var (
	confFile string
)

// 解析命令行参数
func initArgs() {
	//master -config ./master.json
	flag.StringVar(&confFile, "config", "D:\\Go project\\src\\crontab\\master\\main\\master.json", "指定master.json")
	flag.Parse()
}

func main() {
	var err error

	initArgs()

	err = master.InitConfig(confFile)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = master.InitWorkerMgr()
	if err != nil {
		fmt.Println(err)
		return
	}

	err = master.InitLogMgr()
	if err != nil {
		fmt.Println(err)
		return
	}

	err = master.InitJobMgr()
	if err != nil {
		fmt.Println(err)
		return
	}

	err = master.InitApiServer()
	if err != nil {
		fmt.Println(err)
		return
	}

	for {
		time.Sleep(1)
	}
	return
}
