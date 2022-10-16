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
package kafka

import (
	"github.com/Shopify/sarama"
	outputDriver "github.com/brokercap/Bifrost/plugin/driver"
)

// kafka 数据出来之后调用，再进行调用子类的callback函数进行数据解析
func (c *InputKafka) ToChildCallback(kafkaMsg *sarama.ConsumerMessage) {
	if c.childCallBack != nil {
		c.err = c.childCallBack(kafkaMsg)
		// 假如设置了跳过序列化操作，则直接进行跳过
		if c.err != nil {
			if c.config != nil && c.config.SkipSerializeErr == true {
				c.err = nil
				return
			} else {
				// 否则直接退出
				c.Stop()
				c.Close()
			}
		}
	}
}

// 由子类 对kafkaMsg 进行解析完后，再进行回调到此函数
func (c *InputKafka) ToInputCallback(data *outputDriver.PluginDataType) {
	c.callback(data)
	commitEventData := c.BuildCommitEventAndCallback(data)
	c.callback(commitEventData)
}

// 为每一行数据生成一个commit event 事件
func (c *InputKafka) BuildCommitEventAndCallback(data *outputDriver.PluginDataType) *outputDriver.PluginDataType {
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
		BinlogPosition:  data.BinlogPosition,
		Gtid:            data.Gtid,
		Pri:             nil,
		EventID:         c.getNextEventID(),
		ColumnMapping:   nil,
	}
	return newData
}
