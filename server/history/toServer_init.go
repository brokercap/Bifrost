package history

import (
	"github.com/brokercap/Bifrost/server"
	"time"
)

func (This *History) InitToServer() {
	This.Lock()
	defer This.Unlock()
	if len(This.ToServerList) > 0 {
		return
	}
	dbSouceInfo := server.GetDBObj(This.DbName)
	Key := server.GetSchemaAndTableJoin(This.SchemaName, This.TableName)
	for _, toServerInfo := range dbSouceInfo.GetTableSelf(This.SchemaName, This.TableName).ToServerList {
		for _, ID := range This.ToServerIDList {
			if ID == toServerInfo.ToServerID {
				toServerInfoNew := &server.ToServer{
					Key:                &Key,
					ToServerID:         0,
					PluginName:         toServerInfo.PluginName,
					MustBeSuccess:      toServerInfo.MustBeSuccess,
					FilterQuery:        toServerInfo.FilterQuery,
					FilterUpdate:       toServerInfo.FilterUpdate,
					FieldList:          toServerInfo.FieldList,
					ToServerKey:        toServerInfo.ToServerKey,
					BinlogFileNum:      toServerInfo.BinlogFileNum,
					BinlogPosition:     toServerInfo.BinlogPosition,
					PluginParam:        toServerInfo.PluginParam,
					Status:             "",
					ToServerChan:       nil,
					Error:              "",
					ErrorWaitDeal:      0,
					ErrorWaitData:      nil,
					LastBinlogFileNum:  0,     // 由 channel 提交到 ToServerChan 的最后一个位点
					LastBinlogPosition: 0,     // 假如 BinlogFileNum == LastBinlogFileNum && BinlogPosition == LastBinlogPosition 则说明这个位点是没有问题的
					LastBinlogKey:      nil,   // 将数据保存到 level 的key
					QueueMsgCount:      0,     // 队列里的堆积的数量
					FileQueueStatus:    false, // 是否启动文件队列
					Notes:              "history",
				}
				This.ToServerList = append(This.ToServerList, &toServer{threadCount: 0, ToServerInfo: toServerInfoNew})
				break
			}
		}
	}
}

func (This *History) SyncWaitToServerOver(n int) {
	This.Lock()
	defer This.Unlock()
	if This.ToServerTheadGroup != nil {
		This.ToServerTheadGroup.Add(n)
		return
	}
	This.ToServerTheadGroup = NewWaitGroup(n)
	go func() {
		defer func() {
			This.Lock()
			defer This.Unlock()
			This.ToServerTheadGroup = nil
			switch This.Status {
			case HISTORY_STATUS_SELECT_STOPING:
				This.Status = HISTORY_STATUS_SELECT_STOPED
				break
			case HISTORY_STATUS_KILLED, HISTORY_STATUS_HALFWAY:
				break
			default:
				This.Status = HISTORY_STATUS_OVER
				break
			}
		}()
		for {
			This.ToServerTheadGroup.Wait()
			This.Lock()
			if This.selectStatus == true {
				This.Unlock()
				break
			}
			This.Unlock()
			<-time.NewTimer(time.Duration(1) * time.Second).C
		}

	}()
}
