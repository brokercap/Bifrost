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
	pluginDriver "github.com/jc3wish/Bifrost/plugin/driver"
	"github.com/jc3wish/Bifrost/Bristol/mysql"
	"github.com/jc3wish/Bifrost/server/count"
	"github.com/jc3wish/Bifrost/config"
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
	ToServerInfo.LastBinlogFileNum = pluginData.BinlogFileNum
	ToServerInfo.LastBinlogPosition = pluginData.BinlogPosition
	ToServerInfo.Unlock()
	if ToServerInfo.LastBinlogKey == nil{
		ToServerInfo.LastBinlogKey = getToServerLastBinlogkey(This.db,ToServerInfo)
	}
	saveBinlogPosition(ToServerInfo.LastBinlogKey,pluginData.BinlogFileNum,pluginData.BinlogPosition)
	if ToServerInfo.ToServerChan == nil{
		ToServerInfo.ToServerChan = &ToServerChan{
			To:     make(chan *pluginDriver.PluginDataType, config.ToServerQueueSize),
		}
		go ToServerInfo.consume_to_server(This.db,pluginData.SchemaName,pluginData.TableName)
	}
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
	defer func() {
		log.Println("channel",c.Name," consume_channel over; CurrentThreadNum:",c.CurrentThreadNum)
	}()
	for {
		select {
		case data = <-This.c.chanName:
			key := data.SchemaName + "-" + data.TableName
			if This.db.killStatus == 1{
				return
			}
			This.checkChannleStatus()
			toServerList := This.db.tableMap[key].ToServerList
			pluginData := This.transferToPluginData(&data)
			n := len(toServerList)
			if n == 1{
				toServerInfo := toServerList[0]
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
			}else{
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
					//这里要将数据完全拷贝一份出来,因为pluginDriver rows []map[string]interface{} 里map这里在各个toserver 同步到plugin的时候会各自过滤数据。
					//var MyData pluginDriver.PluginDataType
					//err1 := This.deepCopy(&MyData,pluginData)
					//if err1 != nil{
					//	log.Println("consume_to_server deepCopy data:",err1," src data:",data)
					//}
					This.sendToServerResult(toServerInfo,&pluginData)
				}
			}

			if This.db.killStatus == 1{
				return
			}
			c.countChan <- &count.FlowCount{
				//Time:"",
				Count:1,
				TableId:&key,
				ByteSize:int64(data.Header.EventSize)*int64(len(toServerList)),
			}
		case <-time.After(5 * time.Second):
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
