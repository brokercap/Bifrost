package toserver

import (
	"strconv"
	"sync"

	"github.com/Bifrost/toserver/driver"
)

var l sync.Mutex

type ToServer struct {
	sync.Mutex
	Type        string
	ConnUri     string
	Notes       string
	LastID      int
	CurrentConn int
	DataTypeList []string
	TypeAndRule driver.TypeAndRule
}

var ToServerMap map[string]*ToServer

var ToServerConnList map[string]map[string]driver.ConnFun

func init() {
	ToServerMap = make(map[string]*ToServer)
	ToServerConnList = make(map[string]map[string]driver.ConnFun)
}

func GetToServerMap() map[string]*ToServer{
	for _,v := range ToServerMap{
		v.TypeAndRule = driver.GetTypeAndRule(v.Type)
	}
	return ToServerMap
}

func SetToServerInfo(key string, Type string, ConnUri string, Notes string){
	l.Lock()
	if _, ok := ToServerMap[key]; !ok {
		ToServerMap[key] = &ToServer{Type: Type, ConnUri: ConnUri, Notes: Notes, LastID: 0, CurrentConn: 0,TypeAndRule:driver.GetTypeAndRule(Type)}
	}
	l.Unlock()
}

func GetToServerInfo(key string) *ToServer{
	l.Lock()
	defer  l.Unlock()
	if _, ok := ToServerMap[key]; !ok {
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

func Start(key string) driver.ConnFun {
	l.Lock()
	if _, ok := ToServerConnList[key]; !ok {
		ToServerConnList[key] = make(map[string]driver.ConnFun)
	}
	l.Unlock()
	var F driver.ConnFun
	var stringKey string
	F = driver.Open(ToServerMap[key].Type,ToServerMap[key].ConnUri)
	if F == nil{
		return nil
	}

	ToServerMap[key].Lock()
	ToServerMap[key].LastID++
	ToServerMap[key].CurrentConn++
	stringKey = strconv.Itoa(ToServerMap[key].LastID)
	ToServerMap[key].Unlock()

	if stringKey == "" {
		return nil
	}
	l.Lock()
	ToServerConnList[key][stringKey] = F
	l.Unlock()
	return F
}
