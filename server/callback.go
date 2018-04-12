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

	"github.com/Bifrost/Bristol/mysql"
)

func (db *db) Callback(data *mysql.EventReslut) {
	if len(data.Rows) == 0 {
		return
	}
	key := data.SchemaName + "-" + data.TableName
	if _, ok := db.tableMap[key]; !ok {
		return
	}
	ChannelKey := db.tableMap[key].ChannelKey
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
