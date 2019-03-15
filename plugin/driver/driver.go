package driver

import (
	"sync"
	"fmt"
	"regexp"
	"strings"
	"log"
)

func init(){

}

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
	Insert(data *PluginDataType) (bool,error)
	Update(data *PluginDataType) (bool,error)
	Del(data *PluginDataType) (bool,error)
	Query(data *PluginDataType) (bool,error)
	SetParam(p interface{}) error
}

type PluginResult struct {
	BinlogFileName string
	BinlogPosition uint32
}

type driverStructure struct{
	Version string //插件版本
	BifrostVersion string // 插件开发所使用的Bifrost的版本
	Error   string
	Driver  Driver
}

var (
	driversMu sync.RWMutex
	drivers   = make(map[string]driverStructure)
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
	drivers[name] = driverStructure{
		Version:version,
		BifrostVersion:bifrost_version,
		Error:"",
		Driver:driver,
	}
}

func Drivers() []map[string]string {
	driversMu.RLock()
	defer driversMu.RUnlock()
	var list []map[string]string
	for name,v := range drivers {
		m := make(map[string]string)
		m["name"] = name
		m["version"] = v.Version
		m["bifrost_version"] = v.BifrostVersion
		m["error"] = v.Error
		m["uri"] = v.Driver.GetUriExample()
		list = append(list, m)
	}
	return list
}

func Open(name string,uri string) ConnFun{
	driversMu.RLock()
	defer driversMu.RUnlock()
	if _,ok := drivers[name];!ok{
		return nil
	}
	return drivers[name].Driver.Open(uri)
}


func CheckUri(name string,uri string) error{
	driversMu.RLock()
	defer driversMu.RUnlock()
	if _,ok := drivers[name];!ok{
		return fmt.Errorf("no "+name)
	}
	return drivers[name].Driver.CheckUri(uri)
}


const RegularxEpression  = `\{\$([a-zA-Z0-9\-\_]+)\}`

func TransfeResult(val string, data *PluginDataType,rowIndex int) string {
	r, _ := regexp.Compile(RegularxEpression)
	p := r.FindAllStringSubmatch(val, -1)
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
		default:
			val = strings.Replace(val, v[0], fmt.Sprint(data.Rows[rowIndex][v[1]]), -1)
			break
		}
	}
	return val
}
