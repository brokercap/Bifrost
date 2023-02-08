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

package prometheus

import (
	inputDriver "github.com/brokercap/Bifrost/input/driver"
	outputDriver "github.com/brokercap/Bifrost/plugin/driver"
)

func (c *InputPrometheus) SetCallback(callback inputDriver.Callback) {
	c.callback = callback
}

func (c *InputPrometheus) Callback(body []byte) {
	data := NewPrometheusObject(body)
	if data == nil {
		return
	}
	var lastData *outputDriver.PluginDataType
	for data0 := range data.ReadAndToBifrostData() {
		if data0 == nil {
			continue
		}
		data0.EventID = c.getNextEventID()
		c.callback(data0)
		lastData = data0
	}
	if lastData == nil {
		return
	}
	// +1 是为了下一次抓取的时候不重复抓取
	c.config.lastSuccessEndTime = c.config.lastTmpEndTime + 1
	lastData = c.BuildCommitEventAndCallback(lastData)
	c.callback(lastData)
}

// 为每一行数据生成一个commit event 事件

func (c *InputPrometheus) BuildCommitEventAndCallback(data *outputDriver.PluginDataType) *outputDriver.PluginDataType {
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
		BinlogPosition:  uint32(c.config.lastSuccessEndTime),
		Pri:             nil,
		EventID:         c.getNextEventID(),
		ColumnMapping:   nil,
	}
	return newData
}
