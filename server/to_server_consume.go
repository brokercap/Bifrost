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

// 暂停操作
func (This *ToServer) Stop() {
	This.Lock()
	defer This.Unlock()
	if This.Status == "" {
		log.Println("ToServer ",*This.Key,This.ToServerKey,This.ToServerID," stopped")
		This.Status = "stopped"
		return
	}
	if This.Status == "running" {
		log.Println("ToServer ",*This.Key,This.ToServerKey,This.ToServerID," stopping")
		This.Status = "stopping"
		return
	}
}

// 暂停后,重新启动操作
func (This *ToServer) Start() {
	This.Lock()
	defer This.Unlock()
	if This.Status == "stopped" {
		if This.ThreadCount == 0 {
			This.Status = ""
		}else{
			This.statusChan <- true
		}
		log.Println("ToServer ",*This.Key,This.ToServerKey,This.ToServerID," start")
	}
}

func (This *ToServer) ConsumeToServer(db *db,SchemaName string,TableName string)  {
	This.consume_to_server(db,SchemaName,TableName)
}

func (This *ToServer) consume_to_server(db *db,SchemaName string,TableName string) {
	var MyConsumerId uint16
	This.Lock()
	if This.CosumerPluginParamMap == nil{
		This.CosumerPluginParamMap = make(map[uint16]interface{},0)
	}
	This.ThreadCount++
	MyConsumerId = This.CosumerIdInrc
	This.CosumerPluginParamMap[MyConsumerId] = nil
 	This.CosumerIdInrc++
	This.Unlock()
	toServerPositionBinlogKey := getToServerBinlogkey(db,This)
	// 因为有多个地方对 ThreadCount - 1 操作，记录是否已经扣减过
	var ThreadCountDecrDone bool = false
	defer func() {
		if err := recover();err !=nil{
			log.Println(db.Name,This.Notes,"toServerKey:",*This.Key,"MyConsumerId:",MyConsumerId,"SchemaName:",SchemaName,"TableName:",TableName, This.PluginName,This.ToServerKey,"ToServer consume_to_server over;err:",err,"debug",string(debug.Stack()))
			return
		}else{
			log.Println(db.Name,This.Notes,"toServerKey:",*This.Key,"MyConsumerId:",MyConsumerId,"SchemaName:",SchemaName,"TableName:",TableName, This.PluginName,This.ToServerKey,"ToServer consume_to_server over")
		}
		This.Lock()
		if ThreadCountDecrDone == false{
			This.ThreadCount--
		}
		delete(This.CosumerPluginParamMap,MyConsumerId)
		if This.ThreadCount == 0{
			This.CosumerIdInrc = 0
		}
		This.Unlock()
	}()
	log.Println(db.Name,This.Notes,"toServerKey:",*This.Key,"MyConsumerId:",MyConsumerId,"SchemaName:",SchemaName,"TableName:",TableName, This.PluginName,This.ToServerKey,"ToServer consume_to_server  start")
	c := This.ToServerChan.To
	This.Lock()
	if This.Status == "" {
		This.Status = "running"
	}
	This.Unlock()
	var data *pluginDriver.PluginDataType
	var CheckStatusFun = func(){
		for{
			if db.killStatus == 1{
				runtime.Goexit()
			}
			This.Lock()
			switch This.Status {
			case "deling":
				This.Status = "deled"
				delBinlogPosition(toServerPositionBinlogKey)
				This.Unlock()
				runtime.Goexit()
				break
			case "stopping","stopped":
				if This.Status == "stopping" {
					//检测是否需要在暂停操作，如果是暂停操作，则修改为已暂停状态，并且等待开启
					This.Status = "stopped"
					log.Println("ToServer ", *This.Key, This.ToServerKey, This.ToServerID, " stopped")
				}
				This.Unlock()
				select {
				case <- This.statusChan:
					This.Lock()
					This.Status = "running"
					This.Unlock()
					return
				case <- time.NewTimer(5 * time.Second).C:
					break
				}
			default:
				This.Unlock()
				return
			}
		}
	}
	var PluginBinlog *pluginDriver.PluginBinlog
	var errs error
	binlogKey := getToServerBinlogkey(db,This)

	var SaveBinlog = func(){
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
	var fordo int8 = 0
	var lastErrTime int64 = 0
	var warningStatus bool = false

	//告警方法
	var doWarningFun = func(warningType warning.WarningType,body string) {
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
	var checkDoWarning = func() {
		// lastErrTime 是指第一次错误的时间,假如报警过后,将 lastErrTime 修改为1小时后,这样就可以实现最近一小时,同一个错误不会重复报警了
		if time.Now().Unix() - lastErrTime >= 30 {
			lastErrTime = time.Now().Unix()+3600
			doWarningFun(warning.WARNINGERROR,"PluginName:"+This.PluginName+";ToServerKey:"+This.ToServerKey+" err:"+errs.Error())
		}
	}
	var forSendData = func(data *pluginDriver.PluginDataType) {
		for {
			errs = nil
			PluginBinlog,errs = This.sendToServer(data,MyConsumerId)
			if This.MustBeSuccess == true {
				if errs == nil{
					if lastErrTime > 0 {
						This.DelWaitError()
						lastErrTime = 0
						//自动恢复
						doWarningFun(warning.WARNINGNORMAL,"Automatically return to normal")
					}
					fileAck()
					break
				}
				// err != ni 逻辑
				// 假如 lastErrTime == 0 代表已经是第一次循环尝试,则需要记录 错误时间
				if lastErrTime == 0{
					fordo = 0
					This.AddWaitError(errs,data)
					lastErrTime = time.Now().Unix()
				}else{
					// 假如不是第一次循环,尝试 获取 错误信息,是否要被过滤掉,如果要被过滤掉,则退出循环
					dealStatus := This.GetWaitErrorDeal()
					if dealStatus == -1 {
						lastErrTime = 0
						break
					}
					if dealStatus == 1 {
						This.DelWaitError()
						lastErrTime = 0
						//人工处理恢复
						doWarningFun(warning.WARNINGNORMAL, "Return to normal by user")
						break
					}
				}
				fordo++
				// 每重试2次,进行阻塞休眠一次
				if fordo == 2 {
					fordo = 0
					CheckStatusFun()
					timer2 := time.NewTimer(time.Duration(config.PluginSyncRetrycTime) * time.Second)
					<-timer2.C
					timer2.Stop()
					checkDoWarning()
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
	var timer *time.Timer
	timer = time.NewTimer(time.Duration(config.PluginCommitTimeOut) * time.Second)
	defer timer.Stop()
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
		}
		timer.Reset(time.Duration(config.PluginCommitTimeOut) * time.Second)
		select {
		case data = <- c:
			This.Lock()
			This.QueueMsgCount--
			This.Unlock()
			noData = false
			CheckStatusFun()
			warningStatus = false
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
			PluginBinlog, errs = This.commit(MyConsumerId)
			if errs == nil {
				if lastErrTime > 0 {
					This.DelWaitError()
					lastErrTime = 0
					//自动恢复
					doWarningFun(warning.WARNINGNORMAL, "Commit Automatically return to normal")
				}
			} else {
				if This.MustBeSuccess && lastErrTime == 0 {
					This.AddWaitError(errs, data)
					lastErrTime = time.Now().Unix()
					checkDoWarning()
				}
			}
			if noData == false{
				noData = true
				log.Println("consume_to_server:",This.Notes,"toServerKey:",*This.Key,"MyConsumerId:",MyConsumerId,This.PluginName,This.ToServerKey,This.ToServerID," start no data")
			}
			fileAck()
			if PluginBinlog == nil && errs == nil{
				This.Lock()
				if This.QueueMsgCount == 0 {
					// 在全量任务的时候，有可能是起多个消费者,所以这里要判断一下，是不是只剩下一个消费者，只有一个消费者的时候的时候,再将 chan 关闭
					if This.ThreadCount == 1 {
						This.ToServerChan = nil
						This.Status = ""
					}
					//这里要执行一次fileAck ，是为了最终数据一致，将已经从文件中加载出来的数据 ack掉
					PluginBinlog = &pluginDriver.PluginBinlog{fromFileEndBinlogNum, fromFileEndBinlogPosition}
					fileAck()
					// 这里先减一次 This.ThreadCount - 1,是为了防止，defer 执行延时, 其他协程  在进入这个逻辑的时候，继续获取到的值是还没被 -1 的
					This.ThreadCount--
					ThreadCountDecrDone = true
					This.Unlock()
					runtime.Goexit()
				}
				This.Unlock()
			}
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
func (This *ToServer) getPluginAndSetParam(MyConsumerId uint16) (PluginConn pluginDriver.ConnFun,PluginConnKey string,err error){
	PluginConn,PluginConnKey = plugin.GetPlugin(This.ToServerKey)
	if PluginConn == nil{
		return nil,"",fmt.Errorf("Get Plugin:"+This.PluginName+" ToServerKey:"+ This.ToServerKey+ " err,return nil")
	}
	This.RLock()
	if This.CosumerPluginParamMap[MyConsumerId] == nil{
		This.CosumerPluginParamMap[MyConsumerId],err = PluginConn.SetParam(This.PluginParam)
	}else{
		_, err = PluginConn.SetParam(This.CosumerPluginParamMap[MyConsumerId])
	}
	This.RUnlock()
	if err != nil {
		return
	}
	return
}

func (This *ToServer) commit(MyConsumerId uint16) ( Binlog *pluginDriver.PluginBinlog,err error){
	defer func() {
		if err2 := recover();err2 != nil {
			err = fmt.Errorf("ToServer:%s Commit Debug Err:%s",This.ToServerKey,string(debug.Stack()))
			log.Println(This.ToServerKey,"sendToServer err:",err)
		}
	}()

	PluginConn,PluginConnKey,err := This.getPluginAndSetParam(MyConsumerId)
	if err != nil{
		return Binlog,err
	}
	defer plugin.BackPlugin(This.ToServerKey,PluginConnKey,PluginConn)

	Binlog, err = PluginConn.Commit()
	return
}


func (This *ToServer) sendToServer(paramData *pluginDriver.PluginDataType,MyConsumerId uint16) ( Binlog *pluginDriver.PluginBinlog,err error){
	defer func() {
		if err2 := recover();err2 != nil{
			err = fmt.Errorf("sendToServer:%s Commit Debug Err:%s",This.ToServerKey,string(debug.Stack()))
			log.Println(This.ToServerKey,err2,err)
		}
	}()

	// 只有所有字段内容都没有更新，并且开启了过滤功能的情况下，才会返回false
	data,b := This.filterField(paramData)
	if b == false{
		return &pluginDriver.PluginBinlog{data.BinlogFileNum,data.BinlogPosition},nil
	}
	PluginConn,PluginConnKey,err := This.getPluginAndSetParam(MyConsumerId)
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

