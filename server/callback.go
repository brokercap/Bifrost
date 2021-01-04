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
	"log"
	"time"
	"github.com/brokercap/Bifrost/Bristol/mysql"
)

func (db *db) Callback(data *mysql.EventReslut) {
	switch data.Header.EventType {
	case mysql.QUERY_EVENT:
		switch data.Query {
		case "COMMIT":
			db.CallbackDoCommit(data)
			return
		case "BEGIN":
			db.lastTransactionTableMap = make(map[string]map[string]bool,0)
			return
		default:
			break
		}
		break
	case mysql.XID_EVENT:
		db.CallbackDoCommit(data)
		return
	default:
		break
	}
	if db.Callback0(data) == false {
		return
	}
	if _,ok := db.lastTransactionTableMap[data.SchemaName];!ok {
		db.lastTransactionTableMap[data.SchemaName] = make(map[string]bool,0)
	}
	db.lastTransactionTableMap[data.SchemaName][data.TableName] = true
}

func (db *db) CallbackDoCommit(data *mysql.EventReslut) {
	for SchemaName,TableNameMap := range db.lastTransactionTableMap {
		for TableName,_ := range TableNameMap {
			data0 := &mysql.EventReslut{
				Header:         data.Header,
				Rows:           data.Rows,
				Query:          "COMMIT",
				SchemaName:     SchemaName,
				TableName:      TableName,
				BinlogFileName: data.BinlogFileName,
				BinlogPosition: data.BinlogPosition,
				Gtid:			data.Gtid,
				Pri:			data.Pri,
				ColumnMapping:  data.ColumnMapping,
				EventID:		data.EventID,
			}
			db.Callback0(data0)
		}
	}
}

func (db *db) Callback0(data *mysql.EventReslut) (b bool) {
	var ChannelKey int
	var t *Table
	var getChannelKey = func(SchemaName,tableName string) bool {
		t = db.GetTable(SchemaName,tableName)
		if t == nil {
			return false
		}
		ChannelKey = t.ChannelKey
		return true
	}
	//优先判断 全局 *.* 绑定的 channel
	//再判断 schema.* 绑定的 channel
	//最后再判断 schema.table 绑定的 channel
	//这样一样顺序是为了 防止  *.* 绑了的之后,某个表又独立的去绑了其他 channel,防目数据不一致的情况
	for{
		b = getChannelKey("*","*")
		if b {
			break
		}
		b = getChannelKey(data.SchemaName,"*")
		if b {
			break
		}
		b = getChannelKey(data.SchemaName,data.TableName)
		if b {
			break
		}
		//假如没一个获取成功的,直接退出函数
		return
	}
	var i int = 0
	var c *Channel
	for {
		if _, ok := db.channelMap[ChannelKey]; !ok {
			return
		}
		c = db.channelMap[ChannelKey]
		c.RLock()
		if c.Status == CLOSED{
			c.RUnlock()
			return
		}
		if c.Status != RUNNING {
			c.RUnlock()
			if i%600 == 0 {
				log.Printf("ChannelKey:%T , status:%s , data:%T \r\n , ", ChannelKey, c.Status, data)
			}
			time.Sleep(1 * time.Second)
			i++
		} else {
			c.RUnlock()
			break
		}
	}
	chanName := c.GetChannel()
	if chanName != nil {
		chanName <- *data
	} else {
		log.Printf("SchemaName:%s, TableName:%s , ChannelKey:%T chan is nil , data:%T \r\n , ", data.SchemaName,data.TableName, ChannelKey, data)
	}
	return
}