package config 

import (
    "fmt"
    "io/ioutil"
    "errors"
	"gopkg.in/yaml.v2"
)

/**
* 用户权限
*/
type ConfigUser struct {
	User      string `yaml:"user"`
	Pwd       string `yaml:"pwd"`
	Db        string `yaml:"db"`
	Extime    int    `yaml:"extime"`
	ExtimeStr string `yaml:"extime_str"`
}

func ParseConfigUserData(data []byte) (*[]ConfigUser, error) {
	var cfg []ConfigUser
	if err := yaml.Unmarshal([]byte(data), &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func ParseConfigUserFile(fileName string) (*[]ConfigUser, error) {
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
        fmt.Println(err)
		return nil, err
	}
	return ParseConfigUserData(data)
}

func GetUser(fileName,username string)(*ConfigUser, error){
    cfg,err := ParseConfigUserFile(fileName)
    
    if err != nil{
        return nil, err
    }

    for _,u := range *cfg {
        if u.User == username {
            return &u , nil
        }
    }
    return nil,errors.New("not font user")
}

