package kafka

import (
	"encoding/json"

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

func (c *BifrostDataInput) CallBack(kafkaMsg *sarama.Message) error {
	if c.callback == nil {
		return nil
	}
	var data outputDriver.PluginDataType
	c.err = json.Unmarshal(kafkaMsg.Value, &data)
	if c.err != nil {
		return c.err
	}
	c.callback(&data)
	return nil
}
