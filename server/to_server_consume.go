package server

import (
	"time"
	"log"
	"github.com/jc3wish/Bifrost/plugin"
	pluginDriver "github.com/jc3wish/Bifrost/plugin/driver"
	"runtime"
	"fmt"
)

func (This *ToServer) pluginClose(){
	if This.PluginConnKey != ""{
		plugin.Close(This.ToServerKey,This.PluginConnKey)
	}
	This.PluginConn = nil
}

func (This *ToServer) consume_to_server(db *db,SchemaName string,TableName string) {
	defer func() {
		This.pluginClose()
		if err := recover();err !=nil{
			log.Println(db.Name,"SchemaName:",SchemaName,"TableName:",TableName, This.PluginName,This.ToServerKey,"ToServer consume_to_server over;err:",err)
			return
		}else{
			log.Println(db.Name,"SchemaName:",SchemaName,"TableName:",TableName, This.PluginName,This.ToServerKey,"ToServer consume_to_server over")
		}
	}()
	log.Println(db.Name,"SchemaName:",SchemaName,"TableName:",TableName, This.PluginName,This.ToServerKey,"ToServer consume_to_server  start")
	c := This.ToServerChan.To
	var data pluginDriver.PluginDataType
	CheckStatusFun := func(){
		if db.killStatus == 1{
			runtime.Goexit()
		}
		if This.Status == "deling"{
			This.Status = "deled"
			runtime.Goexit()
		}
	}
	for {
		CheckStatusFun()
		select {
		case data = <- c:
			var result bool
			var errs error
			CheckStatusFun()
			var fordo int = 0
			var lastErrId int = 0
			for {
				result = false
				errs = nil
				result,errs = This.sendToServer(data)
				if This.MustBeSuccess == true {
					if result == true{
						if lastErrId > 0 {
							This.DelWaitError()
							lastErrId = 0
						}
						break
					} else {
						if lastErrId > 0{
							dealStatus := This.GetWaitErrorDeal()
							if dealStatus == -1{
								lastErrId = 0
								break
							}
							if dealStatus == 1{
								This.DelWaitError()
								lastErrId = 0
								break
							}
						}else{
							This.AddWaitError(errs,data)
							lastErrId = 1
						}
					}
					fordo++
					if fordo==3{
						CheckStatusFun()
						time.Sleep(2 * time.Second)
						fordo = 0
					}
				}
			}
			//保存位点到toServer配置中，这个 len > toServerInfoKey+1 判断是为了防止在同步过程中 同步配置 被删除所引起的数据不对问题
			db.Lock()
			This.BinlogFileNum = data.BinlogFileNum
			This.BinlogPosition = data.BinlogPosition
			db.Unlock()
			//保存位点,这个位点在重启 配置文件恢复的时候，会根据最小的 ToServerList 的位点进行自动替换
			CheckStatusFun()
		case <-time.After(5 * time.Second):
			//log.Println(time.Now().Format("2006-01-02 15:04:05"))
			//log.Println("count:",count)
		}
	}
}

func (This *ToServer) filterField(data *pluginDriver.PluginDataType){
	n := len(data.Rows)
	if n == 0{
		return
	}
	if len(This.FieldList) == 0{
		return
	}

	if n == 0 {
		m := make(map[string]interface{})
		for _, key := range This.FieldList {
			if _, ok := data.Rows[0][key]; ok {
				m[key] = data.Rows[0][key]
			}
			data.Rows[0] = m
		}
	}else{
		m_before := make(map[string]interface{})
		m_after := make(map[string]interface{})
		for _, key := range This.FieldList {
			if _, ok := data.Rows[0][key]; ok {
				m_before[key] = data.Rows[0][key]
				m_after[key] = data.Rows[1][key]
			}
		}
		data.Rows[0] = m_before
		data.Rows[1] = m_after
	}
}

func (This *ToServer) sendToServer(data pluginDriver.PluginDataType) (result bool,err error){
	defer func() {
		if err2 := recover();err2!=nil{
			result = false
			err = fmt.Errorf(This.ToServerKey,fmt.Sprint(err2))
			log.Println(This.ToServerKey,"sendToServer err:",err2)
			func() {
				defer func() {
					if err2 := recover();err2!=nil{
						return
					}
				}()
				This.PluginConn.Close()
			}()
			This.PluginConn.Connect()
		}
	}()
	if This.PluginConn == nil{
		This.PluginConn,This.PluginConnKey = plugin.Start(This.ToServerKey)
		err := This.PluginConn.SetParam(This.PluginParam)
		if err != nil{
			return false,err
		}
	}

	This.filterField(&data)

	switch data.EventType {
	case "insert":
		result, err = This.PluginConn.Insert(&data)
		break
	case "update":
		result, err = This.PluginConn.Update(&data)
		break
	case "delete":
		result, err = This.PluginConn.Del(&data)
		break
	case "sql":
		result, err = This.PluginConn.Query(&data)
		break
	default:
		break
	}
	return
}

