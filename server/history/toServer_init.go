package history

import (
	"github.com/brokercap/Bifrost/server"
	"log"
)

func (This *History) InitToServer()  {
	This.Lock()
	defer This.Unlock()
	if len(This.ToServerList) > 0{
		return
	}
	dbSouceInfo := server.GetDBObj(This.DbName)
	for _,toServerInfo := range dbSouceInfo.GetTable(This.SchemaName,This.TableName).ToServerList{
		for _,ID := range This.ToServerIDList{
			if ID == toServerInfo.ToServerID{
				toServerInfoNew := &server.ToServer{
					ToServerID			:0,
					PluginName			:toServerInfo.PluginName,
					MustBeSuccess 		:toServerInfo.MustBeSuccess,
					FilterQuery			:toServerInfo.FilterQuery,
					FilterUpdate		:toServerInfo.FilterUpdate,
					FieldList     		:toServerInfo.FieldList,
					ToServerKey   		:toServerInfo.ToServerKey,
					BinlogFileNum 		:toServerInfo.BinlogFileNum,
					BinlogPosition 		:toServerInfo.BinlogPosition,
					PluginParam   		:toServerInfo.PluginParam,
					Status        		:"",
					ToServerChan  		:nil,
					Error		  		:"",
					ErrorWaitDeal 		:0,
					ErrorWaitData 		:nil,
					PluginParamObj 		:nil,
					LastBinlogFileNum   :0,                       // 由 channel 提交到 ToServerChan 的最后一个位点
					LastBinlogPosition  :0,                   	 // 假如 BinlogFileNum == LastBinlogFileNum && BinlogPosition == LastBinlogPosition 则说明这个位点是没有问题的
					LastBinlogKey 		:nil,         			 // 将数据保存到 level 的key
					QueueMsgCount		:0, 					  // 队列里的堆积的数量
					FileQueueStatus 	:false,					  // 是否启动文件队列
					Notes				:"history",
				}
				This.ToServerList = append(This.ToServerList,toServerInfoNew)
				break
			}
		}
	}
}

func (This *History) AsyncWaitToServerOver()  {
	This.Lock()
	defer This.Unlock()
	if This.toServerTheadCountChan != nil{
		return
	}
	This.toServerTheadCountChan = make(chan int16,len(This.ToServerIDList))
	go func() {
		defer func() {
			switch This.Status {
			case HISTORY_STATUS_KILLED, HISTORY_STATUS_HALFWAY:
				break
			default:
				This.Status = HISTORY_STATUS_OVER
				break
			}
			if This.ToServerTheadCount > 0 {
				log.Println("history", This.DbName, This.SchemaName, This.TableName, " over, but sync threadNum:", This.ToServerTheadCount, " != 0")
			}
			This.toServerTheadCountChan = nil
		}()
		var i int16
		for {
			i = <- This.toServerTheadCountChan
			if i == 0 {
				return
			}
			This.ToServerTheadCount += i
			//log.Println("AsyncWaitToServerOver i:",i," This.ToServerTheadCount:",This.ToServerTheadCount)
			if This.ToServerTheadCount <= 0 {
				return
			}
		}
	}()
}