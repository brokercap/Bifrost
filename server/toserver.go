package server

import (
	"sync"
	"log"
	"github.com/jc3wish/Bifrost/plugin/driver"
	"github.com/jc3wish/Bifrost/plugin"

	"encoding/json"
)

type ToServer struct {
	sync.RWMutex
	ToServerID	  		int
	PluginName    		string
	MustBeSuccess 		bool
	FieldList     		[]string
	ToServerKey   		string
	BinlogFileNum 		int
	BinlogPosition 		uint32
	PluginParam   		map[string]interface{}
	Status        		string
	ToServerChan  		*ToServerChan `json:"-"`
	Error		  		string
	ErrorWaitDeal 		int
	ErrorWaitData 		interface{}
	PluginConn	  		driver.ConnFun `json:"-"`
	PluginConnKey 		string `json:"-"`
}

func (db *db) AddTableToServer(schemaName string, tableName string, toserver *ToServer) (bool,int) {
	key := schemaName + "-" + tableName
	if _, ok := db.tableMap[key]; !ok {
		return false,0
	} else {
		db.Lock()
		toserver.ToServerID = db.tableMap[key].LastToServerID + 1
		if toserver.PluginName == ""{
			ToServerInfo := plugin.GetToServerInfo(toserver.ToServerKey)
			if ToServerInfo != nil{
				toserver.PluginName = ToServerInfo.PluginName
			}
		}
		db.tableMap[key].ToServerList = append(db.tableMap[key].ToServerList, toserver)
		db.Unlock()
		log.Println("AddTableToServer",db.Name,schemaName,tableName,toserver)
	}
	return true,toserver.ToServerID
}

func (db *db) DelTableToServer(schemaName string, tableName string,ToServerID int) bool {
	key := schemaName + "-" + tableName
	if _, ok := db.tableMap[key]; !ok {
		return false
	} else {
		var index int = -1
		db.Lock()
		for index1,toServerInfo2 := range db.tableMap[key].ToServerList{
			if toServerInfo2.ToServerID == ToServerID{
				index = index1
				break
			}
		}
		if index == -1 {
			db.Unlock()
			return true
		}
		toServerInfo := db.tableMap[key].ToServerList[index]
		toServerPositionBinlogKey := getToServerBinlogkey(db,toServerInfo)
		//toServerInfo.Lock()
		db.tableMap[key].ToServerList = append(db.tableMap[key].ToServerList[:index], db.tableMap[key].ToServerList[index+1:]...)
		if toServerInfo.Status == "running"{
			toServerInfo.Status = "deling"
		}else{
			if toServerInfo.Status != "deling" {
				delBinlogPosition(toServerPositionBinlogKey)
			}
		}
		log.Println("DelTableToServer",db.Name,schemaName,tableName,"toServerInfo:",toServerInfo)
		db.Unlock()
	}
	return true
}

func (This *ToServer) AddWaitError(WaitErr error,WaitData interface{}) bool {
	This.Lock()
	This.Error = WaitErr.Error()
	b,_:=json.Marshal(WaitData)
	This.ErrorWaitData = string(b)
	This.Unlock()
	return true
}

func (This *ToServer) DealWaitError() bool {
	This.Lock()
	This.ErrorWaitDeal = 1
	This.Unlock()
	return true
}

func (This *ToServer) GetWaitErrorDeal() int {
	This.Lock()
	deal := This.ErrorWaitDeal
	This.Unlock()
	return deal
}

func (This *ToServer) DelWaitError() bool {
	This.Lock()
	This.Error = ""
	This.ErrorWaitData = nil
	This.ErrorWaitDeal = 0
	This.Unlock()
	return true
}