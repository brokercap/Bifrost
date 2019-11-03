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
	if len(data.Rows) == 0 && data.Query == ""{
		return
	}
	var ok bool
	var ChannelKey int
	var key string
	var getChannelKey = func() bool{
		if _, ok = db.tableMap[key]; ok {
			ChannelKey = db.tableMap[key].ChannelKey
			return true
		}
		return false
	}
	//优先判断 全局 *.* 绑定的 channel
	//再判断 schema.* 绑定的 channel
	//最后再判断 schema.table 绑定的 channel
	//这样一样顺序是为了 防止  *.* 绑了的之后,某个表又独立的去绑了其他 channel,防目数据不一致的情况
	for{
		key = AllSchemaAndTablekey
		if getChannelKey() == true{
			break
		}
		key = GetSchemaAndTableJoin(data.SchemaName,"*")
		if getChannelKey() == true{
			break
		}
		key = GetSchemaAndTableJoin(data.SchemaName,data.TableName)
		if getChannelKey() == true{
			break
		}
		//假如没一个获取成功的,直接退出函数
		return
	}
	var i uint = 0
	for {
		if _, ok := db.channelMap[ChannelKey]; !ok {
			return
		}
		if db.channelMap[ChannelKey].Status == "close"{
			return
		}
		if db.channelMap[ChannelKey].Status != "running" {
			if i%600 == 0 {
				log.Printf("ChannelKey:%s , status:%s , data:%s \r\n , ", ChannelKey, db.channelMap[ChannelKey].Status, data)
			}
			time.Sleep(1 * time.Second)
			i++
		} else {
			break
		}
	}
	chanName := db.channelMap[ChannelKey].GetChannel()
	if chanName != nil {
		chanName <- *data
	} else {
		log.Printf("key:%s , ChannelKey:%s chan is nil , data:%s \r\n , ", key, ChannelKey, data)
	}
}

func (db *db) sendToChannel(key string,data *mysql.EventReslut) {

}