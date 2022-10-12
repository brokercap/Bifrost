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

	inputDriver "github.com/brokercap/Bifrost/input/driver"
	outputDriver "github.com/brokercap/Bifrost/plugin/driver"
)

func init() {
	inputDriver.Register("bifrost_kafka", NewBifrostDataInput, VERSION, BIFROST_VERSION)
}

type BifrostDataInput struct {
	Input
}

func NewBifrostDataInput() inputDriver.Driver {
	c := &BifrostDataInput{}
	c.childCallBack = c.CallBack
	return c
}

func (c *BifrostDataInput) CallBack(kafkaMsg *sarama.ConsumerMessage) error {
	if c.callback == nil {
		return nil
	}
	var data outputDriver.PluginDataType
	c.err = json.Unmarshal(kafkaMsg.Value, &data)
	if c.err != nil {
		return c.err
	}
	data.Gtid = c.SetTopicPartitionOffsetAndReturnGTID(kafkaMsg)
	data.EventSize = uint32(len(kafkaMsg.Value))
	data.BinlogFileNum = 1
	data.BinlogPosition = 0
	data.EventID = c.getNextEventID()
	data.AliasSchemaName = kafkaMsg.Topic
	data.AliasTableName = fmt.Sprintf("partition_%d", kafkaMsg.Partition)
	c.callback(&data)
	return nil
}
