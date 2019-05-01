package server

import (
	"time"
	"log"
	"github.com/jc3wish/Bifrost/plugin"
	pluginDriver "github.com/jc3wish/Bifrost/plugin/driver"
	"runtime"
	"fmt"
	"runtime/debug"
	"github.com/jc3wish/Bifrost/server/warning"
)

func (This *ToServer) ConsumeToServer(db *db,SchemaName string,TableName string)  {
	This.consume_to_server(db,SchemaName,TableName)
}

func (This *ToServer) consume_to_server(db *db,SchemaName string,TableName string) {
	toServerPositionBinlogKey := getToServerBinlogkey(db,This)
	defer func() {
		//This.pluginClose()
		if err := recover();err !=nil{
			log.Println(db.Name,"SchemaName:",SchemaName,"TableName:",TableName, This.PluginName,This.ToServerKey,"ToServer consume_to_server over;err:",err,"debug",string(debug.Stack()))
			return
		}else{
			log.Println(db.Name,"SchemaName:",SchemaName,"TableName:",TableName, This.PluginName,This.ToServerKey,"ToServer consume_to_server over")
		}
	}()
	log.Println(db.Name,"SchemaName:",SchemaName,"TableName:",TableName, This.PluginName,This.ToServerKey,"ToServer consume_to_server  start")
	c := This.ToServerChan.To

	var data *pluginDriver.PluginDataType
	CheckStatusFun := func(){
		if db.killStatus == 1{
			runtime.Goexit()
		}
		if This.Status == "deling"{
			This.Status = "deled"
			delBinlogPosition(toServerPositionBinlogKey)
			runtime.Goexit()
		}
	}
	var PluginBinlog *pluginDriver.PluginBinlog
	var errs error
	binlogKey := getToServerBinlogkey(db,This)

	SaveBinlog := func(){
		if PluginBinlog != nil {
			if PluginBinlog.BinlogFileNum == 0{
				return
			}
			//db.Lock()
			This.BinlogFileNum,This.BinlogPosition = PluginBinlog.BinlogFileNum,PluginBinlog.BinlogPosition
			//db.Unlock()
			//这里保存位点是为了刷到磁盘,这个位点在重启 配置文件恢复的时候，会根据最小的 ToServerList 的位点进行自动替换
			saveBinlogPositionByCache(binlogKey, PluginBinlog.BinlogFileNum, PluginBinlog.BinlogPosition)
		}
	}
	var fordo int = 0
	var lastErrId int = 0
	var warningStatus bool = false

	//告警方法
	doWarningFun := func(warningType warning.WarningType,body string) {
		if warningType == warning.WARNINGNORMAL && warningStatus != true {
			return
		}
		warningStatus = true
		warning.AppendWarning(warning.WarningContent{
			Type:       warningType,
			DbName:     db.Name,
			SchemaName: SchemaName,
			TableName:  TableName,
			Body:       body,
		})
	}
	var noData bool = true
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		CheckStatusFun()
		select {
		case data = <- c:
			noData = false
			CheckStatusFun()
			fordo = 0
			lastErrId = 0
			warningStatus = false
			timer.Reset(5  * time.Second)
			for {
				errs = nil
				PluginBinlog,errs = This.sendToServer(data)
				if This.MustBeSuccess == true {
					if errs == nil{
						if lastErrId > 0 {
							This.DelWaitError()
							lastErrId = 0
							//自动恢复
							doWarningFun(warning.WARNINGNORMAL,"Automatically return to normal")
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
								//人工处理恢复
								doWarningFun(warning.WARNINGNORMAL,"Return to normal by user")
								break
							}
						}else{
							This.AddWaitError(errs,data)
							lastErrId = 1
						}
					}
					fordo++
					if fordo % 3 == 0{
						CheckStatusFun()
						//连续15次发送都是失败的,则报警
						if fordo == 15 {
							doWarningFun(warning.WARNINGERROR,"PluginName:"+This.PluginName+";ToServerKey:"+This.ToServerKey+" err:"+errs.Error())
						}
						time.Sleep(2 * time.Second)
					}
				}else{
					PluginBinlog = &pluginDriver.PluginBinlog{data.BinlogFileNum,data.BinlogPosition}
					break
				}
			}
			//这里保存位点，为是了显示的时候，可以直接从内存中读取
			SaveBinlog()
			break
		case <-timer.C:
			timer.Reset(5 * time.Second)
			if noData == false{
				noData = true
				log.Println("consume_to_server:",This.PluginName,This.ToServerKey,This.ToServerID," start no data")
			}
			PluginBinlog,_ = This.commit()
			SaveBinlog()
			break
		}
	}
}

func (This *ToServer) filterField(data *pluginDriver.PluginDataType)(newData *pluginDriver.PluginDataType,b bool){
	n := len(data.Rows)
	if n == 0{
		return data,true
	}
	if len(This.FieldList) == 0{
		return data,true
	}

	if n == 1 {
		m := make(map[string]interface{})
		for _, key := range This.FieldList {
			if _, ok := data.Rows[0][key]; ok {
				m[key] = data.Rows[0][key]
			}
		}
		newData = &pluginDriver.PluginDataType{
			Timestamp:data.Timestamp,
			EventType:data.EventType,
			SchemaName:data.SchemaName,
			TableName:data.TableName,
			BinlogFileNum:data.BinlogFileNum,
			BinlogPosition:data.BinlogPosition,
			Rows:make([]map[string]interface{},1),
		}
		newData.Rows[0] = m
	}else{
		newData = &pluginDriver.PluginDataType{
			Timestamp:data.Timestamp,
			EventType:data.EventType,
			SchemaName:data.SchemaName,
			TableName:data.TableName,
			BinlogFileNum:data.BinlogFileNum,
			BinlogPosition:data.BinlogPosition,
			Rows:make([]map[string]interface{},2),
		}
		m_before := make(map[string]interface{})
		m_after := make(map[string]interface{})
		var isNotUpdate bool = true
		for _, key := range This.FieldList {
			if _, ok := data.Rows[0][key]; ok {
				m_before[key] = data.Rows[0][key]
				m_after[key] = data.Rows[1][key]
				if This.FilterUpdate {
					switch m_after[key].(type) {
					case []string:
						m1 := m_before[key].([]string)
						m2 := m_after[key].([]string)
						n1 := len(m1)
						n2 := len(m2)
						if n1 != n2 {
							isNotUpdate = false
							break
						}
						for k,v := range m1{
							if m2[k] != v{
								isNotUpdate = false
								break
							}
						}
						break
					default:
						if m_before[key] != m_after[key] {
							isNotUpdate = false
						}
						break
					}
				}
			}
		}
		//假如所有字段内容都未变更，并且过滤了这个功能，则直接返回false
		if isNotUpdate && This.FilterUpdate{
			return  data,false
		}
		newData.Rows[0] = m_before
		newData.Rows[1] = m_after
	}
	return newData,true
}

func (This *ToServer) pluginReBack(){
	if This.PluginConn != nil{
		plugin.BackPlugin(This.ToServerKey,This.PluginConnKey,This.PluginConn)
		This.PluginConn = nil
	}
}

//从插件实例池中获取一个插件实例
func (This *ToServer) getPluginAndSetParam() (err error){
	This.PluginConn,This.PluginConnKey = plugin.GetPlugin(This.ToServerKey)
	if This.PluginConn == nil{
		return fmt.Errorf("Get Plugin:"+This.PluginName+" ToServerKey:"+ This.ToServerKey+ " err,return nil")
	}
	if This.PluginParamObj == nil{
		This.PluginParamObj,err = This.PluginConn.SetParam(This.PluginParam)
	}else{
		_, err = This.PluginConn.SetParam(This.PluginParamObj)
	}

	if err != nil {
		return err
	}
	return nil
}

func (This *ToServer) commit() ( Binlog *pluginDriver.PluginBinlog,err error){
	defer func() {
		This.pluginReBack()
		if err2 := recover();err2!=nil{
			err = fmt.Errorf(This.ToServerKey,string(debug.Stack()))
			log.Println(This.ToServerKey,"sendToServer err:",err)
			func() {
				defer func() {
					if err2 := recover();err2!=nil{
						return
					}
				}()
			}()
		}
	}()

	err = This.getPluginAndSetParam()
	if err != nil{
		return Binlog,err
	}

	Binlog, err = This.PluginConn.Commit()
	return
}


func (This *ToServer) sendToServer(paramData *pluginDriver.PluginDataType) ( Binlog *pluginDriver.PluginBinlog,err error){
	defer func() {
		This.pluginReBack()
		if err2 := recover();err2!=nil{
			err = fmt.Errorf(This.ToServerKey,err2,string(debug.Stack()))
			log.Println(This.ToServerKey,"sendToServer err:",err)
			func() {
				defer func() {
					if err2 := recover();err2!=nil{
						return
					}
				}()
			}()
		}
	}()

	// 只有所有字段内容都没有更新，并且开启了过滤功能的情况下，才会返回false
	data,b := This.filterField(paramData)
	if b == false{
		return &pluginDriver.PluginBinlog{data.BinlogFileNum,data.BinlogPosition},nil
	}
	err = This.getPluginAndSetParam()
	if err != nil{
		return Binlog,err
	}

	switch data.EventType {
	case "insert":
		Binlog, err = This.PluginConn.Insert(data)
		break
	case "update":
		Binlog, err = This.PluginConn.Update(data)
		break
	case "delete":
		Binlog, err = This.PluginConn.Del(data)
		break
	case "sql":
		Binlog, err = This.PluginConn.Query(data)
		break
	default:
		break
	}
	return
}

