package driver

import (
	"sync"
	"fmt"
	"regexp"
	"strings"
	"log"
	"encoding/json"
	"time"
)

const API_VERSION  = "v1.0"

func init(){}

type PluginDataType struct {
	Timestamp 		uint32
	EventType 		string
	Rows            []map[string]interface{}
	Query          	string
	SchemaName     	string
	TableName      	string
	BinlogFileNum 	int
	BinlogPosition 	uint32
}

func GetApiVersion() string{
	return API_VERSION
}

type Driver interface {
	Open(uri string) ConnFun
	GetUriExample() string
	CheckUri(uri string) error
}

type ConnFun interface {
	GetConnStatus() string
	SetConnStatus(status string)
	Connect() bool
	ReConnect() bool
	HeartCheck()
	Close() bool
	Insert(data *PluginDataType) (*PluginBinlog,error) //状态,是否立马更新binlog,错误内容
	Update(data *PluginDataType) (*PluginBinlog,error)
	Del(data *PluginDataType) (*PluginBinlog,error)
	Query(data *PluginDataType) (*PluginBinlog,error)
	SetParam(p interface{})(interface{},error)
	Commit() (*PluginBinlog,error)
}

type PluginBinlog struct {
	BinlogFileNum int
	BinlogPosition uint32
}

type ToPluginParam struct {
	FromPluginBinlogChan chan PluginBinlog
}

type DriverStructure struct{
	Version 		string // 插件版本
	BifrostVersion 	string // 插件开发所使用的Bifrost的版本
	Error   		string
	ExampleConnUri 	string
	driver  		Driver
}

var (
	driversMu sync.RWMutex
	drivers   = make(map[string]DriverStructure)
)

func Register(name string, driver Driver,version string,bifrost_version string) {
	defer func() {
		if err := recover();err!=nil{
			log.Println(err)
		}
	}()
	driversMu.Lock()
	defer driversMu.Unlock()
	if driver == nil {
		panic("Register driver is nil")
	}
	if _, ok := drivers[name]; ok {
		panic("Register called twice for driver " + name)
	}
	drivers[name] = DriverStructure{
		Version:version,
		BifrostVersion:bifrost_version,
		Error:"",
		ExampleConnUri:driver.GetUriExample(),
		driver:driver,
	}
}

func Drivers() map[string]DriverStructure {
	driversMu.RLock()
	defer driversMu.RUnlock()
	//json 一次是为了重新拷贝一个内存空间的map出来,防止外部新增修改
	s,err :=json.Marshal(drivers)
	if err != nil{
		return make(map[string]DriverStructure,0)
	}
	var data map[string]DriverStructure
	json.Unmarshal(s,&data)
	return data
}

func Open(name string,uri string) ConnFun{
	driversMu.RLock()
	defer driversMu.RUnlock()
	if _,ok := drivers[name];!ok{
		return nil
	}
	return drivers[name].driver.Open(uri)
}


func CheckUri(name string,uri string) error{
	driversMu.RLock()
	defer driversMu.RUnlock()
	if _,ok := drivers[name];!ok{
		return fmt.Errorf("no "+name)
	}
	return drivers[name].driver.CheckUri(uri)
}

const RegularxEpression  = `\{\$([a-zA-Z0-9\-\_]+)\}`

func TransfeResult(val string, data *PluginDataType,rowIndex int) string {
	r, _ := regexp.Compile(RegularxEpression)
	p := r.FindAllStringSubmatch(val, -1)
	n := len(data.Rows) - 1
	for _, v := range p {
		switch v[1] {
		case "TableName":
			val = strings.Replace(val, "{$TableName}", data.TableName, -1)
			break
		case "SchemaName":
			val = strings.Replace(val, "{$SchemaName}", data.SchemaName, -1)
			break
		case "EventType":
			val = strings.Replace(val, "{$EventType}", data.EventType, -1)
			break
		case "Timestamp":
			val = strings.Replace(val, "{$Timestamp}", fmt.Sprint(time.Now().Unix()), -1)
			break
		default:
			if rowIndex <= n && rowIndex >= 0 {
				val = strings.Replace(val, v[0], fmt.Sprint(data.Rows[rowIndex][v[1]]), -1)
			}else{
				val = strings.Replace(val, v[0], "nil", -1)
			}
			break
		}
	}
	return val
}
