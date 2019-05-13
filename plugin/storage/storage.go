package storage

import (
	"sync"
	"github.com/brokercap/Bifrost/plugin/driver"
	"log"
	"encoding/json"
)

var l sync.RWMutex

type ToServer struct {
	sync.Mutex
	PluginName      string
	PluginVersion 	string
	ConnUri     	string
	Notes       	string
	LastID      	int
	CurrentConn 	int
	MaxConn		    int
	AvailableConn   int
}

var ToServerMap map[string]*ToServer

func init() {
	ToServerMap = make(map[string]*ToServer)
}

func GetToServerMap() map[string]*ToServer{
	return ToServerMap
}

func SetToServerInfo(ToServerKey string,server ToServer){
	Drivers := driver.Drivers();
	if _,ok:=Drivers[server.PluginName];!ok{
		log.Println("SetToServerInfo err: plugin ",ToServerKey," not exsit")
		return
	}
	if server.MaxConn <= 0{
		server.MaxConn = 10
	}
	if server.MaxConn > 500{
		server.MaxConn = 500
	}
	l.Lock()
	if _, ok := ToServerMap[ToServerKey]; !ok {
		ToServerMap[ToServerKey] = &ToServer{
			PluginName: 	server.PluginName,
			PluginVersion:  Drivers[server.PluginName].Version,
			ConnUri: 		server.ConnUri,
			Notes: 			server.Notes,
			LastID: 		0,
			CurrentConn:	0,
			MaxConn:		server.MaxConn,
			AvailableConn:  0,
		}
	}
	l.Unlock()
}

func UpdateToServerInfo(ToServerKey string,server ToServer) error{
	Drivers := driver.Drivers();
	if _,ok:=Drivers[server.PluginName];!ok{
		log.Println("SetToServerInfo err: plugin ",ToServerKey," not exsit")
		return nil
	}
	if server.MaxConn <= 0{
		server.MaxConn = 10
	}
	if server.MaxConn > 500{
		server.MaxConn = 500
	}
	l.Lock()
	if _, ok := ToServerMap[ToServerKey]; ok {
		ToServerMap[ToServerKey].MaxConn = server.MaxConn
		ToServerMap[ToServerKey].Notes = server.Notes
		ToServerMap[ToServerKey].ConnUri = server.ConnUri
	}
	l.Unlock()
	return nil
}


func GetToServerInfo(key string) *ToServer{
	l.Lock()
	defer  l.Unlock()
	if _, ok := ToServerMap[key]; !ok {
		log.Println("ToServer:",key," no exsit,GetToServerInfo nil")
		return nil
	}
	return ToServerMap[key]
}

func DelToServerInfo(key string) bool{
	l.Lock()
	if _, ok := ToServerMap[key]; !ok {
		l.Unlock()
		return true
	}
	delete(ToServerMap,key);
	l.Unlock()
	return true
}


func Recovery(data *json.RawMessage){
	var toData map[string]ToServer
	errors := json.Unmarshal([]byte(*data),&toData)
	if errors != nil{
		log.Println("to server recovry error:",errors)
		return
	}
	for name,v:=range toData{
		SetToServerInfo(name,
			ToServer{
				PluginName:v.PluginName,
				ConnUri:v.ConnUri,
				Notes:v.Notes,
				MaxConn:v.MaxConn,
			})
	}
}

func SaveToServerData() interface{}{
	return ToServerMap
}