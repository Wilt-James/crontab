package master

import (
	"encoding/json"
	"io/ioutil"
)

// 程序配置
type Config struct {
	ApiPort int `json:"apiPort"`
	ApiReadTimeout int `json:"apiReadTimeout"`
	ApiWriteTimeout int `json:"apiWriteTimeout"`
	EtcdEndpoints []string `json:"etcdEndpoints"`
	EtcdDialTimeout int `json:"etcdDialTimeout"`
	WebRoot string `json:"webroot"`
	MongodbUri string `json:"mongodbUri"`
	MongodbConnectTimeout int `json:"mongodbConnectTimeout"`
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