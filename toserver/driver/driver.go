package driver

import (
	"sync"
	"fmt"
)

func init(){

}

type Driver interface {
	Open(uri string) ConnFun
	//GetTypeList() []string
	GetUriExample() string
	CheckUri(uri string) error
	GetTypeAndRule() TypeAndRule
	GetDoc() string
}

type ConnFun interface {
	GetConnStatus() string
	SetConnStatus(status string)
	Connect() bool
	ReConnect() bool
	HeartCheck()
	Close() bool
	Insert(key string, data interface{}) (bool,error)
	Update(key string, data interface{}) (bool,error)
	Del(key string) (bool,error)
	SendToList(key string, data interface{}) (bool,error)
	SetExpir(Expir int)
}

type TypeRule struct {
	Key string
	KeyExample string
	Val string
}

type TypeAndRule struct {
	DataTypeList []string
	TypeList map[string]TypeRule
}

var (
	driversMu sync.RWMutex
	drivers   = make(map[string]Driver)
)

func Register(name string, driver Driver) {
	driversMu.Lock()
	defer driversMu.Unlock()
	if driver == nil {
		panic("sql: Register driver is nil")
	}
	if _, ok := drivers[name]; ok {
		panic("sql: Register called twice for driver " + name)
	}
	drivers[name] = driver
}

func Drivers() []map[string]string {
	driversMu.RLock()
	defer driversMu.RUnlock()
	var list []map[string]string
	for name,v := range drivers {
		m := make(map[string]string)
		m["name"] = name
		m["uri"] = v.GetUriExample()
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
	return drivers[name].Open(uri)
}

func GetTypeAndRule(name string) TypeAndRule{
	driversMu.RLock()
	defer driversMu.RUnlock()
	if _,ok := drivers[name];!ok{
		return TypeAndRule{}
	}
	return drivers[name].GetTypeAndRule()
}

func CheckUri(name string,uri string) error{
	driversMu.RLock()
	defer driversMu.RUnlock()
	if _,ok := drivers[name];!ok{
		return fmt.Errorf("no "+name)
	}
	return drivers[name].CheckUri(uri)
}

func GetDocs() map[string]string{
	driversMu.RLock()
	defer driversMu.RUnlock()
	docs := make(map[string]string,0)
	for name,v := range drivers {
		docs[name] = v.GetDoc()
	}
	return docs
}