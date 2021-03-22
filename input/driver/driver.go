package driver

import (
	"encoding/json"
	"log"
	"runtime/debug"
	"sync"
)

var (
	driversMu sync.RWMutex
	drivers   = make(map[string]DriverStructure)
)

type NewDriver func() Driver

type Driver interface {
	SetOption(inputInfo InputInfo,param map[string]interface{})
	GetUriExample() (string,string)
	Start(ch chan *PluginStatus) error
	Stop() error
	Close() error
	Kill() error
	GetLastPosition() *PluginPosition
	GetCurrentPosition() (*PluginPosition,error)
	Skip(skipEventCount int) error
	SetEventID(eventId uint64) error
	SetCallback(callback Callback)
	CheckPrivilege() error
	CheckUri(CheckPrivilege bool) (CheckUriResult CheckUriResult,err error)
	AddReplicateDoDb(SchemaName,TableName string) (err error)
	DelReplicateDoDb(SchemaName,TableName string) (err error)
	GetVersion() (string,error)

	GetSchemaList() ([]string,error)
	GetSchemaTableList(schema string) (tableList []TableList,err error)
	GetSchemaTableFieldList(schema string, table string) (FieldList []TableFieldInfo,err error)
}

type DriverStructure struct{
	Version 		string // 插件版本
	BifrostVersion 	string // 插件开发所使用的Bifrost的版本
	Error   		string
	ExampleConnUri 	string
	Notes 			string
	driver  		NewDriver
}

func Register(name string, NewDriverFun NewDriver,version string,bifrost_version string) {
	defer func() {
		if err := recover();err != nil {
			log.Println("plugin driver Register name:",name," recory:",err,string(debug.Stack()))
		}
	}()
	driversMu.Lock()
	defer driversMu.Unlock()
	if NewDriverFun == nil {
		panic("Register input driver is nil")
	}
	if _, ok := drivers[name]; ok {
		panic("Register called twice for driver " + name)
	}
	newDriver := NewDriverFun()
	ExampleConnUri,Notes := newDriver.GetUriExample()
	drivers[name] = DriverStructure{
		Version:version,
		BifrostVersion:bifrost_version,
		Error:"",
		ExampleConnUri:ExampleConnUri,
		Notes:Notes,
		driver:NewDriverFun,
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

func Open(name string,inputInfo InputInfo) Driver {
	driversMu.RLock()
	defer driversMu.RUnlock()
	if _, ok := drivers[name]; !ok {
		return nil
	}
	newDriver := drivers[name].driver()
	newDriver.SetOption(inputInfo, nil)
	return newDriver
}