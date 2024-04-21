package storage

import (
	"encoding/json"
	"github.com/brokercap/Bifrost/plugin/driver"
	"log"
	"sync"
	"time"
)

var l sync.RWMutex

type ToServer struct {
	sync.Mutex
	PluginName    string
	PluginVersion string
	ConnUri       string
	Notes         string
	LastID        int   // 每个连接分配一个id，自增的，当前连接池被分配的最大id
	CurrentConn   int   // 当前连接总数
	MaxConn       int   // 连接池最大连接数
	MinConn       int   // 连接池保持最小连接数
	AvailableConn int   // 当前可用连接数
	UpdateTime    int64 // 配置最后修改的时间
}

var ToServerMap map[string]*ToServer

func init() {
	ToServerMap = make(map[string]*ToServer)
}

func GetToServerMap() map[string]*ToServer {
	return ToServerMap
}

func SetToServerInfo(ToServerKey string, server ToServer) {
	Drivers := driver.Drivers()
	if _, ok := Drivers[server.PluginName]; !ok {
		log.Println("SetToServerInfo err: plugin ", ToServerKey, " not exsit")
		return
	}
	if server.MaxConn <= 0 {
		server.MaxConn = 10
	}
	if server.MaxConn > 512 {
		server.MaxConn = 512
	}
	if server.MinConn <= 0 {
		server.MinConn = 1
	}
	if server.MinConn > server.MaxConn {
		server.MinConn = server.MaxConn
	}
	l.Lock()
	if _, ok := ToServerMap[ToServerKey]; !ok {
		ToServerMap[ToServerKey] = &ToServer{
			PluginName:    server.PluginName,
			PluginVersion: Drivers[server.PluginName].Version,
			ConnUri:       server.ConnUri,
			Notes:         server.Notes,
			LastID:        0,
			CurrentConn:   0,
			MaxConn:       server.MaxConn,
			MinConn:       server.MinConn,
			AvailableConn: 0,
			UpdateTime:    time.Now().Unix(),
		}
	}
	l.Unlock()
}

func UpdateToServerInfo(ToServerKey string, server ToServer) error {
	Drivers := driver.Drivers()
	if _, ok := Drivers[server.PluginName]; !ok {
		log.Println("SetToServerInfo err: plugin ", ToServerKey, " not exsit")
		return nil
	}
	if server.MaxConn <= 0 {
		server.MaxConn = 10
	}
	if server.MaxConn > 512 {
		server.MaxConn = 512
	}
	if server.MinConn <= 0 {
		server.MinConn = 1
	}
	if server.MinConn > server.MaxConn {
		server.MinConn = server.MaxConn
	}
	l.Lock()
	if _, ok := ToServerMap[ToServerKey]; ok {
		ToServerMap[ToServerKey].MaxConn = server.MaxConn
		ToServerMap[ToServerKey].Notes = server.Notes
		ToServerMap[ToServerKey].ConnUri = server.ConnUri
		ToServerMap[ToServerKey].MinConn = server.MinConn
		ToServerMap[ToServerKey].UpdateTime = time.Now().UnixNano()
	}
	l.Unlock()
	return nil
}

func GetToServerInfo(key string) *ToServer {
	l.Lock()
	defer l.Unlock()
	if _, ok := ToServerMap[key]; !ok {
		log.Println("ToServer:", key, " no exsit,GetToServerInfo nil")
		return nil
	}
	return ToServerMap[key]
}

func DelToServerInfo(key string) bool {
	l.Lock()
	if _, ok := ToServerMap[key]; !ok {
		l.Unlock()
		return true
	}
	delete(ToServerMap, key)
	l.Unlock()
	return true
}

func Recovery(data *json.RawMessage) {
	var toData map[string]ToServer
	errors := json.Unmarshal([]byte(*data), &toData)
	if errors != nil {
		log.Println("to server recovry error:", errors)
		return
	}
	for name, v := range toData {
		SetToServerInfo(name,
			ToServer{
				PluginName: v.PluginName,
				ConnUri:    v.ConnUri,
				Notes:      v.Notes,
				MaxConn:    v.MaxConn,
				MinConn:    v.MinConn,
				UpdateTime: time.Now().Unix(),
			})
	}
}

func SaveToServerData() interface{} {
	return ToServerMap
}
