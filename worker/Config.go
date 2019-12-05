package worker

import (
	"encoding/json"
	"io/ioutil"
)

// 程序配置
type Config struct {
	EtcdEndpoints []string `json:"etcdEndpoints"`
	EtcdDialTimeout int `json:"etcdDialTimeout"`
	MongodbUri string `json:"mongodbUri"`
	MongodbConnectTimeout int `json:"mongodbConnectTimeout"`
	JobLogBatchSize int `json:"jobLogBatchSize"`
	JobLogCommitTimeout int `json:"jobLogCommitTimeout"`
}

var (
	G_config *Config
)

// 加载配置
func InitConfig(filename string) (err error) {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return
	}

	var conf Config
	err = json.Unmarshal(content, &conf)
	if err != nil {
		return
	}


	G_config = &conf

	return
}