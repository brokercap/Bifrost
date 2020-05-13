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
	"time"
	"strings"
)

import (
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"github.com/brokercap/Bifrost/Bristol/mysql"
	"github.com/brokercap/Bifrost/server/count"
	"github.com/brokercap/Bifrost/config"
	"strconv"
	"sync"
	"log"
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
	}
	return fmt.Sprintf("%d", e)
}

type ToServerChan struct {
	To 		chan *pluginDriver.PluginDataType
}

type consume_channel_obj struct {
	sync.RWMutex
	db      *db
	c       *Channel
	connMap map[string]pluginDriver.ConnFun
}

func NewConsumeChannel(c *Channel) *consume_channel_obj {
	return &consume_channel_obj{
		db:      c.db,
		c:       c,
		connMap: make(map[string]pluginDriver.ConnFun, 0),
	}
}

func (This *consume_channel_obj) checkChannleStatus() {
	if This.c.Status == "close"{
		panic("channel closed")
	}
}

func (This *consume_channel_obj) sendToServerResult(ToServerInfo *ToServer,pluginData *pluginDriver.PluginDataType){
	ToServerInfo.Lock()
	status := ToServerInfo.Status
	FileQueueStatus := ToServerInfo.FileQueueStatus
	if status == "deling" || status == "deled"{
		ToServerInfo.Unlock()
		return
	}
	if status == ""{
		ToServerInfo.Status = "running"
	}
	//修改toserver 对应最后接收的 位点信息
	ToServerInfo.LastBinlogFileNum,ToServerInfo.LastBinlogPosition = pluginData.BinlogFileNum,pluginData.BinlogPosition
	if ToServerInfo.ToServerChan == nil{
		ToServerInfo.ToServerChan = &ToServerChan{
			To:     make(chan *pluginDriver.PluginDataType, config.ToServerQueueSize),
		}
		go ToServerInfo.consume_to_server(This.db,pluginData.SchemaName,pluginData.TableName)
	}
	ToServerInfo.Unlock()
	if ToServerInfo.LastBinlogKey == nil{
		ToServerInfo.LastBinlogKey = getToServerLastBinlogkey(This.db,ToServerInfo)
	}
	saveBinlogPositionByCache(ToServerInfo.LastBinlogKey,pluginData.BinlogFileNum,pluginData.BinlogPosition)
	if FileQueueStatus {
		ToServerInfo.InitFileQueue(This.db.Name,pluginData.SchemaName,pluginData.TableName)
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
				if ToServerInfo.FileQueueUsableCount == 1{
					ToServerInfo.FileQueueUsableCountStartTime = time.Now().UnixNano() / 1e6
				}else{
					// 假如在 FileQueueUsableCountTimeDiff 时间 内 内存队列 被挤满的次数大于 配置的 FileQueueUsableCount 大小，则认为 需要启动文件队列
					// 否则重新开始计算
					if time.Now().UnixNano() / 1e6 - ToServerInfo.FileQueueUsableCountStartTime > config.FileQueueUsableCountTimeDiff {
						if ToServerInfo.FileQueueUsableCount > config.FileQueueUsableCount {
							ToServerInfo.FileQueueStatus = true
						}else{
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
			ToServerInfo.InitFileQueue(This.db.Name, pluginData.SchemaName, pluginData.TableName)
			ToServerInfo.AppendToFileQueue(pluginData)
			ToServerInfo.FileQueueStatus = true
			//log.Println("start FileQueueStatus = true;",*pluginData)
			break
		}
	}else{
		ToServerInfo.ToServerChan.To <- pluginData
		ToServerInfo.Lock()
		ToServerInfo.QueueMsgCount++
		ToServerInfo.Unlock()
	}

}

func (This *consume_channel_obj) transferToPluginData(data *mysql.EventReslut) pluginDriver.PluginDataType{
	i := strings.IndexAny(data.BinlogFileName, ".")
	intString := data.BinlogFileName[i+1:]
	BinlogFileNum,_:=strconv.Atoi(intString)
	return pluginDriver.PluginDataType{
		Timestamp:data.Header.Timestamp,
		EventType:evenTypeName(data.Header.EventType),
		SchemaName:data.SchemaName,
		TableName:data.TableName,
		Rows:data.Rows,
		BinlogFileNum:BinlogFileNum,
		BinlogPosition:data.Header.LogPos,
		Query:data.Query,
		Pri: data.Pri,
	}
}

/*
深度拷贝，性能不是很好，现在放弃了
func(This *consume_channel_obj) deepCopy(dst, src interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		return err
	}
	return gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(dst)
}
*/

func (This *consume_channel_obj) consume_channel() {
	c := This.c
	var data mysql.EventReslut
	log.Println("channel",c.Name," consume_channel start")
	timer := time.NewTimer(5 * time.Second)
	defer func() {
		log.Println("channel",c.Name," consume_channel over; CurrentThreadNum:",c.CurrentThreadNum)
		timer.Stop()
	}()
	var key string
	var countNum int64 = 0
	var AllTableKey string
	var AllTableToServerLen int
	var AllSchemaAndTableToServerLen int
	var TableToServerLen int
	for {
		select {
		case data = <-This.c.chanName:
			if This.db.killStatus == 1{
				return
			}
			timer.Reset(5  * time.Second)
			key = GetSchemaAndTableJoin(data.SchemaName,data.TableName)
			AllTableKey = GetSchemaAndTableJoin(data.SchemaName,"*")
			This.checkChannleStatus()
			pluginData := This.transferToPluginData(&data)
			TableToServerLen 				= This.sendToServerList(key,&pluginData)
			AllTableToServerLen 			= This.sendToServerList(AllTableKey,&pluginData)
			AllSchemaAndTableToServerLen	= This.sendToServerList(AllSchemaAndTablekey,&pluginData)

			if This.db.killStatus == 1{
				return
			}
			switch data.Header.EventType {
			case mysql.UPDATE_ROWS_EVENTv2, mysql.UPDATE_ROWS_EVENTv1, mysql.UPDATE_ROWS_EVENTv0:
				countNum = int64(len(data.Rows)/2)
				break
			case mysql.QUERY_EVENT:
				countNum = 0
				break
			default:
				countNum = int64(len(data.Rows))
				break
			}
			if countNum == 0{
				break
			}
			c.countChan <- &count.FlowCount{
				//Time:"",
				Count:countNum,
				TableId:key,
				ByteSize:int64(data.Header.EventSize)*int64(TableToServerLen),
			}
			if AllTableToServerLen > 0{
				c.countChan <- &count.FlowCount{
					//Time:"",
					Count:countNum,
					TableId:AllTableKey,
					ByteSize:int64(data.Header.EventSize)*int64(AllTableToServerLen),
				}
			}
			if AllSchemaAndTableToServerLen > 0{
				c.countChan <- &count.FlowCount{
					//Time:"",
					Count:countNum,
					TableId:AllSchemaAndTablekey,
					ByteSize:int64(data.Header.EventSize)*int64(AllSchemaAndTableToServerLen),
				}
			}
		case <-timer.C:
			timer.Reset(5 * time.Second)
			//log.Println(time.Now().Format("2006-01-02 15:04:05"))
			//log.Println("count:",countNum)
		}
		for {
			if c.Status == "stop" {
				time.Sleep(1 * time.Second)
			} else {
				break
			}
		}
		if c.CurrentThreadNum > c.MaxThreadNum || c.Status == "close" {
			c.CurrentThreadNum--
			break
		}
	}
}

func (This *consume_channel_obj) sendToServerList(key string,pluginData *pluginDriver.PluginDataType) int{
	if _,ok:=This.db.tableMap[key];!ok{
		return 0
	}
	toServerList := This.db.tableMap[key].ToServerList
	for _, toServerInfo := range toServerList {
		if toServerInfo.FilterQuery && pluginData.EventType == "sql"{
			continue
		}
		if pluginData.BinlogFileNum < toServerInfo.BinlogFileNum{
			continue
		}
		if pluginData.BinlogFileNum == toServerInfo.BinlogFileNum && toServerInfo.BinlogPosition >= pluginData.BinlogPosition{
			continue
		}
		This.sendToServerResult(toServerInfo,pluginData)
	}
	return len(toServerList)
}