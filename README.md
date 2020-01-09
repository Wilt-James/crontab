# 分布式任务调度
## 简介
用Go语言结合etcd实现一个基于master-worker分布式架构的任务调度系统。分布式任务调度项目架构拥有master与worker两种节点，master节点有前端页面web后台，调用后端的HTTP服务器做任务管理。任务会保存到etcd中，并实时同步到worker节点，worker节点收到任务后进入任务调度模块，基于cron表达式做多任务调度。任务到期后交给执行模块并发执行，执行前会尝试获取分布式锁，防止多个协程运行同一个时刻的同一个任务。任务执行完成后日志会存储到异步传输的mongodb中，master节点通过HTTP查询会给web后台提供日志展现。
## 运行方法
- 在master.json与worker.json文件中完成配置或者用-config指定json配置文件路径
- 启动etcd
- 运行master.go与worker.go
