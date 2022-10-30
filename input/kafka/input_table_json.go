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
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"strings"

	inputDriver "github.com/brokercap/Bifrost/input/driver"
	outputDriver "github.com/brokercap/Bifrost/plugin/driver"
)

// 支持整个整个表作为json结构体
func init() {
	inputDriver.Register("table_json_kafka", NewInputTableJsonData, VERSION, BIFROST_VERSION)
}

type InputTableJsonata struct {
	InputKafka
	columnMapping map[string]string
	pri           []string
	tableName     string
	database      string
}

func NewInputTableJsonData() inputDriver.Driver {
	return NewInputTableJsonData0()
}

func NewInputTableJsonData0() *InputTableJsonata {
	c := &InputTableJsonata{}
	c.Init()
	c.childCallBack = c.CallBack
	return c
}

func (c *InputTableJsonata) childInit() {
	if len(c.pri) > 0 {
		return
	}
	if c.config == nil {
		return
	}
	if c.config.ParamMap == nil {
		return
	}
	if _, ok := c.config.ParamMap["input.pri"]; ok {
		c.pri = strings.Split(c.config.ParamMap["input.pri"], ",")
	}
	if _, ok := c.config.ParamMap["input.table"]; ok {
		c.tableName = fmt.Sprint(c.config.ParamMap["input.table"])
	}
	if _, ok := c.config.ParamMap["input.database"]; ok {
		c.database = fmt.Sprint(c.config.ParamMap["input.database"])
	}
}

func (c *InputTableJsonata) CallBack(kafkaMsg *sarama.ConsumerMessage) error {
	if c.callback == nil {
		return nil
	}
	if len(kafkaMsg.Value) == 0 {
		return nil
	}
	c.childInit()
	var msgData map[string]interface{}
	err := json.Unmarshal(kafkaMsg.Value, &msgData)
	if err != nil {
		return err
	}
	var SchemaName, TableName string
	if c.tableName == "" {
		TableName = c.FormatPartitionTableName(kafkaMsg.Partition)
	} else {
		TableName = c.tableName
	}
	if c.database == "" {
		SchemaName = kafkaMsg.Topic
	} else {
		SchemaName = c.database
	}

	data := &outputDriver.PluginDataType{
		Timestamp:       uint32(kafkaMsg.Timestamp.Unix()),
		EventSize:       uint32(len(kafkaMsg.Value)),
		EventType:       "insert",
		Rows:            []map[string]interface{}{msgData},
		Query:           "",
		SchemaName:      SchemaName,
		TableName:       TableName,
		AliasSchemaName: kafkaMsg.Topic,
		AliasTableName:  c.FormatPartitionTableName(kafkaMsg.Partition),
		BinlogFileNum:   1,
		BinlogPosition:  0,
		Gtid:            c.SetTopicPartitionOffsetAndReturnGTID(kafkaMsg),
		EventID:         c.getNextEventID(),
		ColumnMapping:   nil,
		Pri:             c.pri,
	}
	c.ToInputCallback(data)
	return nil
}
