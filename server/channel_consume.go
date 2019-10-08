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
	"bytes"
	"encoding/gob"
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
	if status == "deling" || status == "deled"{
		ToServerInfo.Unlock()
		return
	}
	if status == ""{
		ToServerInfo.Status = "running"
	}
	//修改toserver 对应最后接收的 位点信息
	ToServerInfo.LastBinlogFileNum,ToServerInfo.LastBinlogPosition = pluginData.BinlogFileNum,pluginData.BinlogPosition
	ToServerInfo.QueueMsgCount++
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
	ToServerInfo.ToServerChan.To <- pluginData
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
	}
}

func(This *consume_channel_obj) deepCopy(dst, src interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		return err
	}
	return gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(dst)
}

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
	var count int64 = 0
	for {
		select {
		case data = <-This.c.chanName:
			if This.db.killStatus == 1{
				return
			}
			timer.Reset(5  * time.Second)
			key = GetSchemaAndTableJoin(data.SchemaName,data.TableName)
			This.checkChannleStatus()
			toServerList := This.db.tableMap[key].ToServerList
			pluginData := This.transferToPluginData(&data)
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
				This.sendToServerResult(toServerInfo,&pluginData)
			}

			if This.db.killStatus == 1{
				return
			}
			switch data.Header.EventType {
			case mysql.UPDATE_ROWS_EVENTv2, mysql.UPDATE_ROWS_EVENTv1, mysql.UPDATE_ROWS_EVENTv0:
				count = int64(len(data.Rows)/2)
				break
			case mysql.QUERY_EVENT:
				count = 0
				break
			default:
				count = int64(len(data.Rows))
				break
			}
			if count == 0{
				break
			}
			c.countChan <- &count.FlowCount{
				//Time:"",
				Count:count,
				TableId:key,
				ByteSize:int64(data.Header.EventSize)*int64(len(toServerList)),
			}
		case <-timer.C:
			timer.Reset(5 * time.Second)
			//log.Println(time.Now().Format("2006-01-02 15:04:05"))
			//log.Println("count:",count)
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
