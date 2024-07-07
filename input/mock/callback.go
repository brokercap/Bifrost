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

package mock

import (
	inputDriver "github.com/brokercap/Bifrost/input/driver"
	outputDriver "github.com/brokercap/Bifrost/plugin/driver"
	"time"
)

func (c *InputMock) SetCallback(callback inputDriver.Callback) {
	c.callback = callback
}

func (c *InputMock) Callback(data *outputDriver.PluginDataType) {
	c.lastEventEndTime = uint32(time.Now().Unix())
	data.Timestamp = c.lastEventEndTime
	data.EventID = c.getNextEventID()
	data.AliasSchemaName, data.AliasTableName = data.SchemaName, data.TableName
	c.callback(data)
	commitData := c.BuildCommitEventAndCallback(data)
	c.callback(commitData)
}

// 为每一行数据生成一个commit event 事件

func (c *InputMock) BuildCommitEventAndCallback(data *outputDriver.PluginDataType) *outputDriver.PluginDataType {
	newData := &outputDriver.PluginDataType{
		Timestamp:       data.Timestamp,
		EventSize:       5,
		EventType:       "commit",
		Rows:            nil,
		Query:           "",
		SchemaName:      data.SchemaName,
		TableName:       data.TableName,
		AliasSchemaName: data.AliasSchemaName,
		AliasTableName:  data.AliasTableName,
		BinlogFileNum:   data.BinlogFileNum,
		BinlogPosition:  c.lastEventEndTime,
		Pri:             nil,
		EventID:         c.getNextEventID(),
		ColumnMapping:   nil,
	}
	return newData
}
