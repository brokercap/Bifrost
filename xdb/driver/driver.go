package driver

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
)

var (
	driversMu sync.RWMutex
	drivers   = make(map[string]DriverStructure)
)

type DriverStructure struct {
	Version string // 版本
	Error   string
	driver  Driver
}

type Driver interface {
	Open(uri string) (XdbDriver, error)
}

type XdbDriver interface {
	GetKeyVal(key []byte) ([]byte, error)
	PutKeyVal(key []byte, val []byte) error
	DelKeyVal(key []byte) error
	GetListByKeyPrefix(key []byte) ([]ListValue, error)
	Close() error
}

type ListValue struct {
	Key   string
	Value string
}

func Register(name string, driver Driver, version string) {
	defer func() {
		if err := recover(); err != nil {
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
		Version: version,
		Error:   "",
		driver:  driver,
	}
}

func Drivers() map[string]DriverStructure {
	driversMu.RLock()
	defer driversMu.RUnlock()
	//json 一次是为了重新拷贝一个内存空间的map出来,防止外部新增修改
	s, err := json.Marshal(drivers)
	if err != nil {
		return make(map[string]DriverStructure, 0)
	}
	var data map[string]DriverStructure
	json.Unmarshal(s, &data)
	return data
}

func Open(name string, uri string) (XdbDriver, error) {
	driversMu.RLock()
	defer driversMu.RUnlock()
	if _, ok := drivers[name]; !ok {
		return nil, fmt.Errorf(name + " not exsit")
	}
	return drivers[name].driver.Open(uri)
}
