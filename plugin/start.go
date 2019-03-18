package plugin

import (
	"strconv"
	"sync"

	"github.com/jc3wish/Bifrost/plugin/driver"
	"log"
	"runtime/debug"
)

var l sync.Mutex

type ToServer struct {
	sync.Mutex
	PluginName      string
	PluginVersion 	string
	ConnUri     	string
	Notes       	string
	LastID      	int
	CurrentConn 	int
}

var ToServerMap map[string]*ToServer

var ToServerConnList map[string]map[string]driver.ConnFun

func init() {
	ToServerMap = make(map[string]*ToServer)
	ToServerConnList = make(map[string]map[string]driver.ConnFun)
}

func GetToServerMap() map[string]*ToServer{
	return ToServerMap
}

func SetToServerInfo(key string, PluginName string, ConnUri string, Notes string){
	Drivers := driver.Drivers();
	if _,ok:=Drivers[PluginName];!ok{
		log.Println("SetToServerInfo err: plugin ",key," not exsit")
		return
	}
	l.Lock()
	if _, ok := ToServerMap[key]; !ok {
		ToServerMap[key] = &ToServer{
			PluginName: 	PluginName,
			PluginVersion:  Drivers[PluginName].Version,
			ConnUri: 		ConnUri,
			Notes: 			Notes,
			LastID: 		0,
			CurrentConn:	0,
		}
	}
	l.Unlock()
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

func Start(key string) (driver.ConnFun,string) {
	l.Lock()
	if _, ok := ToServerConnList[key]; !ok {
		ToServerConnList[key] = make(map[string]driver.ConnFun)
	}
	l.Unlock()
	if _,ok := ToServerMap[key];!ok{
		log.Println("ToServer:",key," no exsit,Start error")
		return nil,""
	}
	var F driver.ConnFun
	var stringKey string
	F = driver.Open(ToServerMap[key].PluginName,ToServerMap[key].ConnUri)
	if F == nil{
		return nil,""
	}

	ToServerMap[key].Lock()
	ToServerMap[key].LastID++
	ToServerMap[key].CurrentConn++
	stringKey = strconv.Itoa(ToServerMap[key].LastID)
	ToServerMap[key].Unlock()

	if stringKey == "" {
		return nil,""
	}
	l.Lock()
	ToServerConnList[key][stringKey] = F
	l.Unlock()
	return F,stringKey
}

func Close(key string,stringKey string) bool {
	defer func() {
		if err := recover();err !=nil{
			log.Println(string(debug.Stack()))
			return
		}
	}()
	l.Lock()
	defer l.Unlock()
	if _, ok := ToServerConnList[key]; !ok {
		return true
	}
	if _, ok := ToServerMap[key]; !ok {
		return true
	}
	if _, ok := ToServerConnList[key][stringKey]; !ok {
		return true
	}
	ToServerMap[key].CurrentConn--
	ToServerConnList[key][stringKey].Close()
	return true
}
