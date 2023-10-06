/*
Copyright [2018] [jc3wish]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package server

import (
	"fmt"
	"github.com/brokercap/Bifrost/Bristol/mysql"
	"github.com/brokercap/Bifrost/config"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"github.com/brokercap/Bifrost/server/count"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

func evenTypeName(e mysql.EventType) string {
	switch e {
	case mysql.WRITE_ROWS_EVENTv0, mysql.WRITE_ROWS_EVENTv1, mysql.WRITE_ROWS_EVENTv2:
		return "insert"
	case mysql.UPDATE_ROWS_EVENTv0, mysql.UPDATE_ROWS_EVENTv1, mysql.UPDATE_ROWS_EVENTv2:
		return "update"
	case mysql.DELETE_ROWS_EVENTv0, mysql.DELETE_ROWS_EVENTv1, mysql.DELETE_ROWS_EVENTv2:
		return "delete"
	case mysql.QUERY_EVENT:
		return "sql"
	case mysql.XID_EVENT:
		return "commit"
	default:
		break
	}
	return fmt.Sprintf("%d", e)
}

type ToServerChan struct {
	To chan *pluginDriver.PluginDataType
}

type consume_channel_obj struct {
	sync.RWMutex
	db         *db
	c          *Channel
	SchemaName string
	TableName  string
}

func NewConsumeChannel(c *Channel) *consume_channel_obj {
	return &consume_channel_obj{
		db: c.db,
		c:  c,
	}
}

func (This *consume_channel_obj) checkChannleStatus() {
	if This.c.Status == CLOSED {
		panic("channel closed")
	}
}

func (This *consume_channel_obj) sendToServerResult(ToServerInfo *ToServer, pluginData *pluginDriver.PluginDataType) {
	ToServerInfo.Lock()
	status := ToServerInfo.Status
	FileQueueStatus := ToServerInfo.FileQueueStatus
	if status == DELING || status == DELED {
		ToServerInfo.Unlock()
		return
	}
	if status == DEFAULT {
		ToServerInfo.Status = RUNNING
	}
	//修改toserver 对应最后接收的 位点信息
	var lastQueueBinlog = &PositionStruct{
		BinlogFileNum:  pluginData.BinlogFileNum,
		BinlogPosition: pluginData.BinlogPosition,
		GTID:           pluginData.Gtid,
		Timestamp:      pluginData.Timestamp,
		EventID:        pluginData.EventID,
	}
	ToServerInfo.LastQueueBinlog = lastQueueBinlog

	// 支持到 1.8.x
	ToServerInfo.LastBinlogFileNum, ToServerInfo.LastBinlogPosition = pluginData.BinlogFileNum, pluginData.BinlogPosition

	//ToServerInfo.LastBinlogFileNum,ToServerInfo.LastBinlogPosition,ToServerInfo.LastBinlogGtid,ToServerInfo.LastBinlogEventID = pluginData.BinlogFileNum,pluginData.BinlogPosition,pluginData.Gtid,pluginData.EventID
	if ToServerInfo.ToServerChan == nil {
		ToServerInfo.ToServerChan = &ToServerChan{
			To: make(chan *pluginDriver.PluginDataType, config.ToServerQueueSize),
		}
		go ToServerInfo.consume_to_server(This.db, pluginData.SchemaName, pluginData.TableName)
	}
	ToServerInfo.Unlock()
	if ToServerInfo.LastBinlogKey == nil {
		ToServerInfo.LastBinlogKey = getToServerLastBinlogkey(This.db, ToServerInfo)
	}
	saveBinlogPositionByCache(ToServerInfo.LastBinlogKey, lastQueueBinlog)
	if FileQueueStatus {
		ToServerInfo.InitFileQueue(This.db.Name, pluginData.SchemaName, pluginData.TableName)
		ToServerInfo.AppendToFileQueue(pluginData)
		return
	}

	// 假如开启了全局文件队列的功能,假如 规定时间内 都没写进内存chan队列,则往文件队列中写数据
	if config.FileQueueUsable {
		timer := time.NewTimer(time.Duration(config.FileQueueUsableCountTimeDiff) * time.Millisecond)
		defer timer.Stop()
		select {
		case ToServerInfo.ToServerChan.To <- pluginData:
			ToServerInfo.Lock()
			ToServerInfo.QueueMsgCount++
			if int(ToServerInfo.QueueMsgCount) >= config.ToServerQueueSize {
				ToServerInfo.FileQueueUsableCount++
				if ToServerInfo.FileQueueUsableCount == 1 {
					ToServerInfo.FileQueueUsableCountStartTime = time.Now().UnixNano() / 1e6
				} else {
					// 假如在 FileQueueUsableCountTimeDiff 时间 内 内存队列 被挤满的次数大于 配置的 FileQueueUsableCount 大小，则认为 需要启动文件队列
					// 否则重新开始计算
					if time.Now().UnixNano()/1e6-ToServerInfo.FileQueueUsableCountStartTime > config.FileQueueUsableCountTimeDiff {
						if ToServerInfo.FileQueueUsableCount > config.FileQueueUsableCount {
							ToServerInfo.FileQueueStatus = true
						} else {
							ToServerInfo.FileQueueUsableCount = 0
						}
					}
				}
			}
			ToServerInfo.Unlock()
			break
		case <-timer.C:
			ToServerInfo.Lock()
			defer ToServerInfo.Unlock()
			ToServerInfo.InitFileQueue(This.db.Name, This.SchemaName, This.TableName)
			ToServerInfo.AppendToFileQueue(pluginData)
			ToServerInfo.FileQueueStatus = true
			//log.Println("start FileQueueStatus = true;",*pluginData)
			break
		}
	} else {
		ToServerInfo.ToServerChan.To <- pluginData
		ToServerInfo.Lock()
		ToServerInfo.QueueMsgCount++
		ToServerInfo.Unlock()
	}

}

func (This *consume_channel_obj) transferToPluginData(data *mysql.EventReslut) (pluginData *pluginDriver.PluginDataType) {
	i := strings.IndexAny(data.BinlogFileName, ".")
	intString := data.BinlogFileName[i+1:]
	BinlogFileNum, _ := strconv.Atoi(intString)
	pluginData = &pluginDriver.PluginDataType{
		Timestamp:      data.Header.Timestamp,
		EventType:      evenTypeName(data.Header.EventType),
		SchemaName:     data.SchemaName,
		TableName:      data.TableName,
		Rows:           data.Rows,
		BinlogFileNum:  BinlogFileNum,
		BinlogPosition: data.Header.LogPos,
		Query:          data.Query,
		Gtid:           data.Gtid,
		Pri:            data.Pri,
		ColumnMapping:  data.ColumnMapping,
		EventID:        data.EventID,
	}
	return
}

func (This *consume_channel_obj) consumeChannel() {
	c := This.c
	var pluginData *pluginDriver.PluginDataType
	log.Println("channel", c.Name, " consume_channel start")
	timer := time.NewTimer(5 * time.Second)
	defer func() {
		log.Println("channel", c.Name, " consume_channel over; CurrentThreadNum:", c.CurrentThreadNum)
		timer.Stop()
	}()
	var key string
	var AllTableKey string
	var countNum int64 = 0
	var EventSize int64 = 0
	for {
		select {
		case pluginData = <-This.c.chanName:
			if This.db.killStatus == 1 {
				return
			}
			This.checkChannleStatus()

			switch pluginData.EventType {
			case "update":
				countNum = int64(len(pluginData.Rows) / 2)
				break
			case "sql", "commit":
				countNum = 0
				break
			default:
				countNum = int64(len(pluginData.Rows))
				break
			}
			EventSize = int64(pluginData.EventSize)

			key = GetSchemaAndTableJoin(pluginData.AliasSchemaName, pluginData.AliasTableName)
			AllTableKey = GetSchemaAndTableJoin(pluginData.AliasSchemaName, "*")
			//pluginData := This.transferToPluginData(&data)
			This.SchemaName, This.TableName = pluginData.AliasSchemaName, pluginData.AliasTableName
			This.sendToServerList(key, pluginData, countNum, EventSize)
			This.SchemaName, This.TableName = pluginData.AliasSchemaName, "*"
			This.sendToServerList(AllTableKey, pluginData, countNum, EventSize)
			This.SchemaName, This.TableName = "*", "*"
			This.sendToServerList(AllSchemaAndTablekey, pluginData, countNum, EventSize)

			if This.db.killStatus == 1 {
				return
			}

			timer.Reset(5 * time.Second)
		case <-timer.C:
			timer.Reset(5 * time.Second)
		}
		for {
			if c.Status == STOPPED {
				time.Sleep(1 * time.Second)
			} else {
				break
			}
		}
		if c.CurrentThreadNum > c.MaxThreadNum || c.Status == CLOSED {
			c.CurrentThreadNum--
			break
		}
	}
}

func (This *consume_channel_obj) checkIgnoreTable(t *Table, TableName string) bool {
	This.db.RLock()
	if len(t.doTableMap) > 0 {
		if _, ok := t.doTableMap[TableName]; ok {
			This.db.RUnlock()
			return false
		}
		This.db.RUnlock()
		return true
	}
	if _, ok := t.ignoreTableMap[TableName]; ok {
		This.db.RUnlock()
		return true
	}
	This.db.RUnlock()
	return false
}

func (This *consume_channel_obj) sendToServerList(key string, pluginData *pluginDriver.PluginDataType, countNum int64, EventSize int64) {
	t := This.db.GetTableByKey(key)
	if t == nil {
		return
	}
	if This.checkIgnoreTable(t, pluginData.TableName) == false {
		if len(t.ToServerList) > 0 {
			This.sendToServerList0(t.ToServerList, pluginData)
			This.c.countChan <- &count.FlowCount{
				Count:    countNum,
				TableId:  t.key,
				ByteSize: EventSize * int64(len(t.ToServerList)),
			}
		}
	}
	for _, t0 := range t.likeTableList {
		if This.checkIgnoreTable(t0, pluginData.TableName) == true {
			continue
		}
		This.sendToServerList0(t0.ToServerList, pluginData)
		This.c.countChan <- &count.FlowCount{
			Count:    countNum,
			TableId:  t0.key,
			ByteSize: EventSize * int64(len(t0.ToServerList)),
		}
	}
}

func (This *consume_channel_obj) sendToServerList0(toServerList []*ToServer, pluginData *pluginDriver.PluginDataType) {
	for _, toServerInfo := range toServerList {
		if toServerInfo.FilterQuery && pluginData.EventType == "sql" {
			if pluginData.Query != "COMMIT" {
				continue
			}
		}
		if pluginData.EventID < toServerInfo.LastSuccessBinlog.EventID {
			// 这里多加一层 时间差过滤, 防止在数据的时候，EventID 计算错误造成可能丢失的bug
			// 这里直接 continue 过滤只是尽可能防止重复同步而已
			if pluginData.Timestamp < toServerInfo.LastSuccessBinlog.Timestamp {
				continue
			}
		}
		/*
			if pluginData.BinlogFileNum < toServerInfo.BinlogFileNum {
				continue
			}
			if pluginData.Timestamp < toServerInfo.LastSuccessBinlog.Timestamp || (pluginData.BinlogFileNum == toServerInfo.LastSuccessBinlog.BinlogFileNum && toServerInfo.LastSuccessBinlog.BinlogPosition >= pluginData.BinlogPosition) {
				continue
			}
		*/
		This.sendToServerResult(toServerInfo, pluginData)
	}
}
