package server

import (
	"fmt"
	"github.com/brokercap/Bifrost/plugin"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"github.com/brokercap/Bifrost/server/warning"
	"io"
	"log"
	"runtime"
	"runtime/debug"
	"time"
	"github.com/brokercap/Bifrost/config"
)

func (This *ToServer) ConsumeToServer(db *db,SchemaName string,TableName string)  {
	This.consume_to_server(db,SchemaName,TableName)
}

func (This *ToServer) consume_to_server(db *db,SchemaName string,TableName string) {
	This.Lock()
	This.ThreadCount++
	This.Unlock()
	toServerPositionBinlogKey := getToServerBinlogkey(db,This)
	defer func() {
		//This.pluginClose()
		if err := recover();err !=nil{
			log.Println(db.Name,This.Notes,"SchemaName:",SchemaName,"TableName:",TableName, This.PluginName,This.ToServerKey,"ToServer consume_to_server over;err:",err,"debug",string(debug.Stack()))
			return
		}else{
			log.Println(db.Name,This.Notes,"SchemaName:",SchemaName,"TableName:",TableName, This.PluginName,This.ToServerKey,"ToServer consume_to_server over")
		}
		This.Lock()
		This.ThreadCount--
		This.Unlock()
	}()
	log.Println(db.Name,This.Notes,"SchemaName:",SchemaName,"TableName:",TableName, This.PluginName,This.ToServerKey,"ToServer consume_to_server  start")
	c := This.ToServerChan.To
	This.Lock()
	if This.Status == ""{
		This.Status = "running"
	}
	This.Unlock()
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
	var commitErrorCount int = 0


	var unack int = 0      // 在一次遍历中从文件队列中加载出来的数量，需要
	var tmpUnack int = 0   // 在一次遍历中从文件队列中加载出来 但是实际位点是被成功处理过后的 数量
	var fromFileEndBinlogNum int = 0   // 从文件队列中加载出来的最后 位点
	var fromFileEndBinlogPosition uint32 = 0  // 从文件队列中加载出来的最后 位点
	var fileTotalCount int = 0
	var fileAck = func() {
		if PluginBinlog == nil{
			return
		}
		if unack == 0{
			return
		}
		if This.fileQueueObj == nil{
			unack = 0
			return
		}
		if PluginBinlog.BinlogFileNum == 0 || PluginBinlog.BinlogPosition == 0{
			return
		}
		//假如 最后成功的位点，大于文件中加载的位点，则将所有待从文件中的数量  ack 掉
		if PluginBinlog.BinlogFileNum > fromFileEndBinlogNum{
			//log.Println("file ackn:",unack," fileTotalCount:",fileTotalCount)
			This.fileQueueObj.Ack(unack)
			unack = 0
			return
		}
		if PluginBinlog.BinlogFileNum == fromFileEndBinlogNum{
			if PluginBinlog.BinlogPosition >= fromFileEndBinlogPosition{
				//log.Println("file ackn2:",unack," and unack:",unack," fileTotalCount:",fileTotalCount)
				This.fileQueueObj.Ack(unack)
				unack = 0
			}else{
				This.fileQueueObj.Ack(1)
				unack--
				//log.Println("file ack1:",1," and unack:",unack," fileTotalCount:",fileTotalCount)
			}
		}else{
			return
		}
	}
	var forSendData = func(data *pluginDriver.PluginDataType) {
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
					fileAck()
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
				fileAck()
				break
			}
		}
	}
	var n1 int = 0
	var n0 int = 0
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	//time.Sleep(20 * time.Second)
	for {
		CheckStatusFun()
		if This.FileQueueStatus && This.QueueMsgCount == 0{
			//这要问我这里为什么 -1, 因为我不知道 在同一个线程里写满后再消费，会不会进入 chan 死锁的情况
			queueVariableSize := config.ToServerQueueSize - 1
			if queueVariableSize > 0{
				This.InitFileQueue(db.Name, SchemaName, TableName)
				//log.Println("file ack2:",unack," fileTotalCount:",fileTotalCount)
				This.fileQueueObj.Ack(unack)
				tmpUnack = 0
				unack = 0
				log.Println(db.Name, SchemaName, TableName,This.PluginName,This.ToServerKey,"ToServer consume_to_server start PopFileQueue")
				for i:=0; i < queueVariableSize; i++ {
					data0, err := This.PopFileQueue()
					if err != nil && err != io.EOF {
						doWarningFun(warning.WARNINGERROR, "PluginName:"+This.PluginName+";ToServerKey:"+This.ToServerKey+";dbName:"+db.Name+";SchemaName:"+SchemaName+";TableName:"+TableName+"; PopFileQueue err:"+err.Error())
						log.Println(db.Name, SchemaName, TableName, ";ToServerKey:"+This.ToServerKey, " PopFileQueue err:", err, " restart Bifrost please!")
						panic("PluginName:" + This.PluginName + ";ToServerKey:" + This.ToServerKey + ";dbName:" + db.Name + ";SchemaName:" + SchemaName + ";TableName:" + TableName + "; PopFileQueue err:" + err.Error())
					}
					if data0 == nil && err == nil {
						// 说明没有数据可以加载了
						This.Lock()
						This.FileQueueStatus = false
						This.Unlock()
						break
					} else {
						/*
						if i == 0{
							log.Println("PopFileQueue first: ",*data0)
						}
						*/
						// 这里为什么要判断一下位点，是因为文件队列是要整个文件的数据都被从加载到内存后才会 删除文件
						// 那有一种可能，一个文件还没被完全加载完，进程就被重启了呢？那重启后，是不是旧的数据会被重新读取吗？
						if data0.BinlogFileNum < This.BinlogFileNum{
							tmpUnack++
							continue
						}
						if data0.BinlogFileNum == This.BinlogFileNum && data0.BinlogPosition <= This.BinlogPosition {
							tmpUnack++
							continue
						}
						fromFileEndBinlogNum,fromFileEndBinlogPosition = data0.BinlogFileNum,data0.BinlogPosition
						unack++
						fileTotalCount++
						This.QueueMsgCount++
						c <- data0
					}
				}
				This.fileQueueObj.Ack(tmpUnack)
				//假如这一次循环加载出来的数据，全是已经同步过的，则继续从文件中加载
				if unack == 0{
					continue
				}
			}
			timer.Reset(5  * time.Second)
		}
		select {
		case data = <- c:
			This.Lock()
			This.QueueMsgCount--
			This.Unlock()
			noData = false
			CheckStatusFun()
			fordo = 0
			lastErrId = 0
			warningStatus = false
			timer.Reset(5  * time.Second)
			switch data.EventType {
			case "sql":
				forSendData(data)
				break
			case "insert","delete":
				n1 = len(data.Rows)
				if n1 > 1{
					n0 = 0
					for _,v := range data.Rows{
						n0++
						d := &pluginDriver.PluginDataType{
							Timestamp:data.Timestamp,
							EventType:data.EventType,
							Query:"",
							SchemaName:data.SchemaName,
							TableName:data.TableName,
							BinlogFileNum:0,
							BinlogPosition:0,
							Rows:make([]map[string]interface{},1),
							Pri: data.Pri,
						}
						if n0 == n1{
							d.BinlogFileNum = data.BinlogFileNum
							d.BinlogPosition = data.BinlogPosition
						}
						d.Rows[0] = v
						forSendData(d)
					}
				}else{
					forSendData(data)
				}
				break
			case "update":
				n1 = len(data.Rows)
				if n1 > 2{
					for n0 = 0;n0 < n1;n0+=2{
						d := &pluginDriver.PluginDataType{
							Timestamp:data.Timestamp,
							EventType:data.EventType,
							Query:"",
							SchemaName:data.SchemaName,
							TableName:data.TableName,
							BinlogFileNum:0,
							BinlogPosition:0,
							Rows:make([]map[string]interface{},2),
							Pri: data.Pri,
						}
						if n0 == n1-2{
							d.BinlogFileNum = data.BinlogFileNum
							d.BinlogPosition = data.BinlogPosition
						}
						d.Rows[0] = data.Rows[n0]
						d.Rows[1] = data.Rows[n0+1]
						forSendData(d)
					}
				}else{
					forSendData(data)
				}
				break
			default:
				forSendData(data)
				break
			}
			//这里保存位点，为是了显示的时候，可以直接从内存中读取
			SaveBinlog()
			break
		case <-timer.C:
			PluginBinlog, errs = This.commit()
			if errs == nil {
				if commitErrorCount > 0 {
					commitErrorCount = 0
					This.DelWaitError()
					lastErrId = 0
					//自动恢复
					doWarningFun(warning.WARNINGNORMAL, "Commit Automatically return to normal")
				}
			} else {
				This.AddWaitError(errs, data)
				commitErrorCount++
				//连续6次发送都是失败的,则报警
				if commitErrorCount == 6 {
					doWarningFun(warning.WARNINGERROR, "Commit PluginName:"+This.PluginName+";ToServerKey:"+This.ToServerKey+" err:"+errs.Error())
				}
			}
			if noData == false{
				noData = true
				log.Println("consume_to_server:",This.Notes,This.PluginName,This.ToServerKey,This.ToServerID," start no data")
			}
			fileAck()
			if PluginBinlog == nil && errs == nil{
				This.Lock()
				if This.QueueMsgCount == 0{
					This.ToServerChan = nil
					This.Status = ""
					This.Unlock()
					//这里要执行一次fileAck ，是为了最终数据一致，将已经从文件中加载出来的数据 ack掉
					PluginBinlog = &pluginDriver.PluginBinlog{fromFileEndBinlogNum,fromFileEndBinlogPosition}
					fileAck()
					runtime.Goexit()
				}
				This.Unlock()
			}
			timer.Reset(5 * time.Second)
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

//从插件实例池中获取一个插件实例
func (This *ToServer) getPluginAndSetParam() (PluginConn pluginDriver.ConnFun,PluginConnKey string,err error){
	PluginConn,PluginConnKey = plugin.GetPlugin(This.ToServerKey)
	if PluginConn == nil{
		return nil,"",fmt.Errorf("Get Plugin:"+This.PluginName+" ToServerKey:"+ This.ToServerKey+ " err,return nil")
	}
	if This.PluginParamObj == nil{
		This.PluginParamObj,err = PluginConn.SetParam(This.PluginParam)
	}else{
		_, err = PluginConn.SetParam(This.PluginParamObj)
	}

	if err != nil {
		return
	}
	return
}

func (This *ToServer) commit() ( Binlog *pluginDriver.PluginBinlog,err error){
	defer func() {
		if err2 := recover();err2 != nil {
			err = fmt.Errorf("ToServer:%s Commit Debug Err:%s",This.ToServerKey,string(debug.Stack()))
			log.Println(This.ToServerKey,"sendToServer err:",err)
		}
	}()

	PluginConn,PluginConnKey,err := This.getPluginAndSetParam()
	if err != nil{
		return Binlog,err
	}
	defer plugin.BackPlugin(This.ToServerKey,PluginConnKey,PluginConn)

	Binlog, err = PluginConn.Commit()
	return
}


func (This *ToServer) sendToServer(paramData *pluginDriver.PluginDataType) ( Binlog *pluginDriver.PluginBinlog,err error){
	defer func() {
		if err2 := recover();err2 != nil{
			err = fmt.Errorf("sendToServer:%s Commit Debug Err:%s",This.ToServerKey,string(debug.Stack()))
			log.Println(This.ToServerKey,"sendToServer err:",err)
		}
	}()

	// 只有所有字段内容都没有更新，并且开启了过滤功能的情况下，才会返回false
	data,b := This.filterField(paramData)
	if b == false{
		return &pluginDriver.PluginBinlog{data.BinlogFileNum,data.BinlogPosition},nil
	}
	PluginConn,PluginConnKey,err := This.getPluginAndSetParam()
	if err != nil{
		return Binlog,err
	}
	defer plugin.BackPlugin(This.ToServerKey,PluginConnKey,PluginConn)

	switch data.EventType {
	case "insert":
		Binlog, err = PluginConn.Insert(data)
		break
	case "update":
		Binlog, err = PluginConn.Update(data)
		break
	case "delete":
		Binlog, err = PluginConn.Del(data)
		break
	case "sql":
		Binlog, err = PluginConn.Query(data)
		break
	default:
		break
	}
	return
}

