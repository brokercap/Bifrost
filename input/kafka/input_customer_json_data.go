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
	"log"
	"runtime/debug"
	"strings"

	inputDriver "github.com/brokercap/Bifrost/input/driver"
	outputDriver "github.com/brokercap/Bifrost/plugin/driver"
)

const InputCustomerJsonData = "customer_json_kafka"

func init() {
	inputDriver.Register(InputCustomerJsonData, NewCustomerJsonDataInput, VERSION, BIFROST_VERSION)
}

type CustomerJsonDataInput struct {
	InputKafka
	pluginCustomerDataObj *outputDriver.PluginDataCustomerJson
	hadTransferConfig     bool
}

func NewCustomerJsonDataInput() inputDriver.Driver {
	return NewCustomerJsonDataInput0()
}

func NewCustomerJsonDataInput0() *CustomerJsonDataInput {
	c := &CustomerJsonDataInput{}
	c.Init()
	c.childCallBack = c.CallBack
	c.pluginCustomerDataObj, _ = outputDriver.NewPluginDataCustomerJson()
	return c
}

func (c *CustomerJsonDataInput) TransferConfig() {
	if c.hadTransferConfig {
		return
	}
	defer func() {
		c.hadTransferConfig = true
	}()

	key2rowPath := c.tansferConfigKey2Row(c.getConfig("input.key2row"))
	c.pluginCustomerDataObj.SetKey2Row(key2rowPath)

	databasePath := c.tansferConfigPath(c.getConfig("input.database"))
	c.pluginCustomerDataObj.SetDatabasePath(databasePath)

	tablePath := c.tansferConfigPath(c.getConfig("input.table"))
	c.pluginCustomerDataObj.SetTablePath(tablePath)

	pksPath := c.tansferConfigPath(c.getConfig("input.pks"))
	c.pluginCustomerDataObj.SetPksPath(pksPath)

	updateNewDataPath := c.tansferConfigPath(c.getConfig("input.update_new_data"))
	c.pluginCustomerDataObj.SetUpdateNewDataPath(updateNewDataPath)

	updateOldDataPath := c.tansferConfigPath(c.getConfig("input.update_old_data"))
	c.pluginCustomerDataObj.SetUpdateOldDataPath(updateOldDataPath)

	insertDataPath := c.tansferConfigPath(c.getConfig("input.insert_data"))
	c.pluginCustomerDataObj.SetInsertDataPath(insertDataPath)

	deleteDataPath := c.tansferConfigPath(c.getConfig("input.delete_data"))
	c.pluginCustomerDataObj.SetDeleteDataPath(deleteDataPath)

	eventTypePath := c.tansferConfigPath(c.getConfig("input.event.type"))
	c.pluginCustomerDataObj.SetEventTypePath(eventTypePath)

	if _, ok := c.config.ParamMap["input.event.type.val.insert"]; ok {
		c.pluginCustomerDataObj.SetEventTypeValInsert(c.config.ParamMap["input.event.type.val.insert"])
	}
	if _, ok := c.config.ParamMap["input.event.type.val.select"]; ok {
		c.pluginCustomerDataObj.SetEventTypeValSelect(c.config.ParamMap["input.event.type.val.select"])
	}
	if _, ok := c.config.ParamMap["input.event.type.val.update"]; ok {
		c.pluginCustomerDataObj.SetEventTypeValUpdate(c.config.ParamMap["input.event.type.val.update"])
	}
	if _, ok := c.config.ParamMap["input.event.type.val.delete"]; ok {
		c.pluginCustomerDataObj.SetEventTypeValDelete(c.config.ParamMap["input.event.type.val.delete"])
	}
}

func (c *CustomerJsonDataInput) getConfig(key string) *string {
	if val, ok := c.config.ParamMap[key]; ok {
		return &val
	}
	return nil
}

func (c *CustomerJsonDataInput) tansferConfigKey2Row(config *string) (key2Row []outputDriver.PluginCustomerJsonDataKey2Row) {
	if config == nil {
		return
	}
	tmpArr := strings.Split(*config, ",")
	for _, v := range tmpArr {
		tmpArr0 := strings.Split(v, ":")
		if len(tmpArr0) > 2 {
			log.Printf("[ERROR] input:%s childInit input.key2row: %s is not valid,use like a.b:name please! \n", InputCustomerJsonData, v)
			continue
		}
		var name string
		if len(tmpArr0) == 2 {
			name = tmpArr0[1]
		} else {
			name = tmpArr0[0]
		}
		keyPath := strings.Split(tmpArr0[0], ".")
		key2Row = append(key2Row, outputDriver.PluginCustomerJsonDataKey2Row{Name: name, Path: keyPath})
	}
	return
}

func (c *CustomerJsonDataInput) tansferConfigPath(config *string) (path []string) {
	if config == nil {
		return
	}
	path = strings.Split(*config, ".")
	return
}

func (c *CustomerJsonDataInput) CallBack(kafkaMsg *sarama.ConsumerMessage) error {
	if c.callback == nil {
		return nil
	}
	c.TransferConfig()
	defer func() {
		if err := recover(); err != nil {
			log.Printf("%s CallBack recover err:%+v \n", InputCustomerJsonData, err)
			log.Println(string(debug.Stack()))
		}
	}()
	err := c.pluginCustomerDataObj.Decoder(kafkaMsg.Value)
	if err != nil {
		return err
	}
	data := c.pluginCustomerDataObj.ToBifrostOutputPluginData()
	if data == nil {
		log.Printf("[ERROR] input:%s ToBifrostOutputPluginData nil, kafkaMsg:%+v \n", InputCustomerJsonData, string(kafkaMsg.Value))
		return nil
	}
	data.Gtid = c.SetTopicPartitionOffsetAndReturnGTID(kafkaMsg)
	data.EventSize = uint32(len(kafkaMsg.Value))
	data.BinlogFileNum = 1
	data.BinlogPosition = 0
	data.EventID = c.getNextEventID()
	data.AliasSchemaName = kafkaMsg.Topic
	data.AliasTableName = c.FormatPartitionTableName(kafkaMsg.Partition)
	c.ToInputCallback(data)
	return nil
}
