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

	inputDriver "github.com/brokercap/Bifrost/input/driver"
	outputDriver "github.com/brokercap/Bifrost/plugin/driver"
)

func init() {
	inputDriver.Register("string_kafka", NewInputStringData, VERSION, BIFROST_VERSION)
}

type InputStringData struct {
	InputKafka
	columnMapping map[string]string
	pri           []string
}

func NewInputStringData() inputDriver.Driver {
	return NewInputStringData0()
}

func NewInputStringData0() *InputStringData {
	c := &InputStringData{}
	c.Init()
	c.childCallBack = c.CallBack
	c.columnMapping = map[string]string{
		"queue_key":       "text",
		"queue_value":     "text",
		"queue_topic":     "varchar(256)",
		"queue_timestamp": "timestamp",
		"queue_partition": "int32",
		"queue_offset":    "int64",
	}
	c.pri = []string{"queue_topic", "queue_partition", "queue_offset"}
	return c
}

func (c *InputStringData) CallBack(kafkaMsg *sarama.ConsumerMessage) error {
	if c.callback == nil {
		return nil
	}
	msgData := map[string]interface{}{
		"queue_key":       string(kafkaMsg.Key),
		"queue_value":     string(kafkaMsg.Value),
		"queue_timestamp": kafkaMsg.Timestamp.Format("2006-01-02 15:04:05"),
		"queue_topic":     kafkaMsg.Topic,
		"queue_partition": kafkaMsg.Partition,
		"queue_offset":    kafkaMsg.Offset,
	}
	TableName := c.FormatPartitionTableName(kafkaMsg.Partition)
	data := &outputDriver.PluginDataType{
		Timestamp:       uint32(kafkaMsg.Timestamp.Unix()),
		EventSize:       uint32(len(kafkaMsg.Value)),
		EventType:       "insert",
		Rows:            []map[string]interface{}{msgData},
		Query:           "",
		SchemaName:      kafkaMsg.Topic,
		TableName:       TableName,
		AliasSchemaName: kafkaMsg.Topic,
		AliasTableName:  TableName,
		BinlogFileNum:   1,
		BinlogPosition:  0,
		Gtid:            c.SetTopicPartitionOffsetAndReturnGTID(kafkaMsg),
		EventID:         c.getNextEventID(),
		ColumnMapping:   c.columnMapping,
		Pri:             c.pri,
	}
	c.ToInputCallback(data)
	return nil
}
