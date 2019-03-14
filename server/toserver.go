package server

import (
	"sync"
	"log"
	"time"
	"github.com/jc3wish/Bifrost/plugin/driver"
	"github.com/jc3wish/Bifrost/plugin"

	"encoding/json"
)

type ToServer struct {
	sync.RWMutex
	ToServerID	  int
	ToServerType  string
	MustBeSuccess bool
	FieldList     []string
	ToServerKey   string
	BinlogFileNum int
	BinlogPosition uint32
	PluginParam   map[string]interface{}
	Status        string
	ToServerChan  *ToServerChan `json:"-"`
	Error		  string
	ErrorWaitDeal int
	ErrorWaitData interface{}
	PluginConn	  driver.ConnFun `json:"-"`
	PluginConnKey string `json:"-"`
}

func (db *db) AddTableToServer(schemaName string, tableName string, toserver *ToServer) bool {
	key := schemaName + "-" + tableName
	if _, ok := db.tableMap[key]; !ok {
		return false
	} else {
		db.Lock()
		toserver.ToServerID = db.tableMap[key].LastToServerID + 1
		if toserver.ToServerType == ""{
			ToServerInfo := plugin.GetToServerInfo(toserver.ToServerKey)
			if ToServerInfo != nil{
				toserver.ToServerType = ToServerInfo.Type
			}
		}
		db.tableMap[key].ToServerList = append(db.tableMap[key].ToServerList, toserver)
		db.Unlock()
		log.Println("AddTableToServer",db.Name,schemaName,tableName,toserver)
	}
	return true
}

func (db *db) DelTableToServer(schemaName string, tableName string, index int,ToServerID int) bool {
	key := schemaName + "-" + tableName
	if _, ok := db.tableMap[key]; !ok {
		return false
	} else {
		db.Lock()
		if len(db.tableMap[key].ToServerList) < index-1{
			db.Unlock()
			return true
		}
		var del bool = false
		toServerInfo := db.tableMap[key].ToServerList[index]
		if toServerInfo.ToServerID != ToServerID{
			return false
		}
		toServerInfo.Lock()
		if toServerInfo.Status == "running"{
			del = true
			toServerInfo.Status = "deling"
		}else{
			db.tableMap[key].ToServerList=append(db.tableMap[key].ToServerList[:index],db.tableMap[key].ToServerList[index+1:]...)
		}
		toServerInfo.Unlock()
		db.Unlock()
		if del == true {
			go func() {
				for {
					time.Sleep(2 * time.Second)
					if toServerInfo.Status == "deled" {
						db.Lock()
						//这里要重新遍历一次 并且和 ToServerID 做对比, 是因为有可能在在这个是时候,index 已经变更过，但是ToServerID 又是唯一值
						// ToServerList 不采用map 而采用 list 的原因 有一个重要原因,是因为 想实现每次在发送数据的时候，是按顺序的
						for index,toServerInfo := range db.tableMap[key].ToServerList{
							if toServerInfo.ToServerID != ToServerID{
								continue
							}
							db.tableMap[key].ToServerList = append(db.tableMap[key].ToServerList[:index], db.tableMap[key].ToServerList[index+1:]...)
						}
						db.Unlock()
						return
					}
				}
			}()
		}
		log.Println("DelTableToServer",db.Name,schemaName,tableName,"toServerInfo:",toServerInfo)
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