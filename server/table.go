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
	"sync"
)

type Table struct {
	sync.RWMutex
	key            string // schema+table 组成的key
	Name           string
	ChannelKey     int
	LastToServerID int
	ToServerList   []*ToServer
	likeTableList  []*Table        // 关联了哪些 模糊匹配的配置
	regexpErr      bool            // 是否执行正则表达式错误，如果 true，则下一次不会再执行，直接错过
	IgnoreTable    string          // 假如是模糊匹配的时候，指定某些表不进行匹配，逗号隔开
	ignoreTableMap map[string]bool // 指定某些表不进行匹配的表数据 map 格式
	DoTable        string
	doTableMap     map[string]bool // 指定某些表进行匹配的表数据 map 格式
}

func AddTable(db, schema, tableName, IgnoreTable string, DoTable string, channelId int) error {
	if _, ok := DbList[db]; !ok {
		return fmt.Errorf(db + " not exsit")
	}
	if DbList[db].AddTable(schema, tableName, IgnoreTable, DoTable, channelId, 0) == true {
		return nil
	}
	return fmt.Errorf("unkown error")
}

func UpdateTable(db, schema, tableName, IgnoreTable string, DoTable string) error {
	if _, ok := DbList[db]; !ok {
		return fmt.Errorf(db + " not exsit")
	}
	if DbList[db].UpdateTable(schema, tableName, IgnoreTable, DoTable) == true {
		return nil
	}
	return fmt.Errorf("unkown error")
}

func DelTable(db, schema, tableName string) error {
	if _, ok := DbList[db]; !ok {
		return fmt.Errorf(db + "not exsit")
	}
	DbList[db].DelTable(schema, tableName)
	return nil
}

func AddTableToServer(db, schemaName, tableName string, ToServerInfo ToServer) error {
	if _, ok := DbList[db]; !ok {
		return fmt.Errorf(db + "not exsit")
	}
	key := GetSchemaAndTableJoin(schemaName, tableName)
	if _, ok := DbList[db].tableMap[key]; !ok {
		return fmt.Errorf(key + " not exsit")
	} else {
		DbList[db].AddTableToServer(schemaName, tableName, &ToServerInfo)
	}
	return nil
}
