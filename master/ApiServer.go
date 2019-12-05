package master

import (
	"crontab/common"
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"time"
)

type ApiServer struct {
	httpServer *http.Server
}

var (
	G_apiServer *ApiServer
)

// 保存任务接口
// POST job = {"name":"job1", "command":"echo hello", "cronExpr":"*/5 * * * * * *"}
func handleJobSave(resp http.ResponseWriter, req *http.Request) {

	err := req.ParseForm()
	if err != nil {
		if bytes, err := common.BuildResponse(-1, err.Error(), nil); err == nil {
			resp.Write(bytes)
		}
		return
	}

	postJob := req.PostForm.Get("job")

	var job common.Job
	err = json.Unmarshal([]byte(postJob), &job)
	if err != nil {
		if bytes, err := common.BuildResponse(-1, err.Error(), nil); err == nil {
			resp.Write(bytes)
		}
		return
	}

	oldJob, err := G_jobMgr.SaveJob(&job)
	if err != nil {
		if bytes, err := common.BuildResponse(-1, err.Error(), nil); err == nil {
			resp.Write(bytes)
		}
		return
	}

	if bytes, err := common.BuildResponse(0, "success", oldJob); err == nil {
		resp.Write(bytes)
	}

	return

}

// 删除任务接口
// POST /job/delete name=job1
func handleJobDelete(resp http.ResponseWriter, req *http.Request) {
	err := req.ParseForm()
	if err != nil {
		if bytes, err := common.BuildResponse(-1, err.Error(), nil); err == nil {
			resp.Write(bytes)
		}
		return
	}

	name := req.PostForm.Get("name")

	oldJob, err := G_jobMgr.DeleteJob(name)
	if err != nil {
		if bytes, err := common.BuildResponse(-1, err.Error(), nil); err == nil {
			resp.Write(bytes)
		}
		return
	}

	if bytes, err := common.BuildResponse(0, "success", oldJob); err == nil {
		resp.Write(bytes)
	}
}

// 列举所有crontab任务
func handleJobList(resp http.ResponseWriter, req *http.Request) {
	jobList, err := G_jobMgr.ListJobs()
	if err != nil {
		if bytes, err := common.BuildResponse(-1, err.Error(), nil); err == nil {
			resp.Write(bytes)
		}
		return
	}

	if bytes, err := common.BuildResponse(0, "success", jobList); err == nil {
		resp.Write(bytes)
	}

	return

}

// 强制杀死某个任务
// POST /job/kill name=job1
func handleJobKill(resp http.ResponseWriter, req *http.Request) {
	err := req.ParseForm()
	if err != nil {
		if bytes, err := common.BuildResponse(-1, err.Error(), nil); err == nil {
			resp.Write(bytes)
		}
		return
	}

	name := req.PostForm.Get("name")

	err = G_jobMgr.KillJob(name)
	if err != nil {
		if bytes, err := common.BuildResponse(-1, err.Error(), nil); err == nil {
			resp.Write(bytes)
		}
		return
	}

	if bytes, err := common.BuildResponse(0, "success", nil); err == nil {
		resp.Write(bytes)
	}

	return
}

// 查询任务日志
func handleJobLog(resp http.ResponseWriter, req *http.Request) {
	err := req.ParseForm()
	if err != nil {
		if bytes, err := common.BuildResponse(-1, err.Error(), nil); err == nil {
			resp.Write(bytes)
		}
		return
	}

	// 获取请求参数 /job/log?name=job10&skip=0&limit=10
	name := req.Form.Get("name") // 任务名字
	skipParam := req.Form.Get("skip") // 从第几条开始
	limitParam := req.Form.Get("limit") // 返回多少条
	skip, err := strconv.Atoi(skipParam)
	if err != nil {
		skip = 0
	}
	limit, err := strconv.Atoi(limitParam)
	if err != nil {
		limit = 20
	}
	logArr, err := G_logMgr.ListLog(name, skip, limit)
	if err != nil {
		if bytes, err := common.BuildResponse(-1, err.Error(), nil); err == nil {
			resp.Write(bytes)
		}
		return
	}

	// 正常应答
	if bytes, err := common.BuildResponse(0, "success", logArr); err == nil {
		resp.Write(bytes)
	}
	return
}

// 获取健康worker节点列表
func handleWorkerList(resp http.ResponseWriter, req *http.Request) {
	workerArr, err := G_workerMgr.ListWorkers()
	if err != nil {
		if bytes, err := common.BuildResponse(-1, err.Error(), nil); err == nil {
			resp.Write(bytes)
		}
		return
	}

	if bytes, err := common.BuildResponse(0, "success", workerArr); err == nil {
		resp.Write(bytes)
	}

}

// 初始化服务
func InitApiServer() (err error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/job/save", handleJobSave)
	mux.HandleFunc("/job/delete", handleJobDelete)
	mux.HandleFunc("/job/list", handleJobList)
	mux.HandleFunc("/job/kill", handleJobKill)
	mux.HandleFunc("/job/log", handleJobLog)
	mux.HandleFunc("/worker/list", handleWorkerList)

	staticDir := http.Dir(G_config.WebRoot)
	staticHandler := http.FileServer(staticDir)
	mux.Handle("/", http.StripPrefix("/", staticHandler))

	listener, err := net.Listen("tcp", ":" + strconv.Itoa(G_config.ApiPort))
	if err != nil {
		return
	}

	httpServer := &http.Server{
		ReadTimeout:  time.Duration(G_config.ApiReadTimeout) * time.Millisecond,
		WriteTimeout: time.Duration(G_config.ApiWriteTimeout) * time.Millisecond,
		Handler:      mux,
	}

	G_apiServer = &ApiServer{
		httpServer: httpServer,
	}

	go httpServer.Serve(listener)

	return
}
