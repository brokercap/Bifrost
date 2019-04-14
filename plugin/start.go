package plugin

import (
	"sync"
	"github.com/jc3wish/Bifrost/plugin/driver"
	"log"
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
	if server.MaxConn == 0{
		server.MaxConn = 10
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

/*

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

*/