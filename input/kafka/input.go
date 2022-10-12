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
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Shopify/sarama"

	inputDriver "github.com/brokercap/Bifrost/input/driver"

)

type waitCommitOffset struct {
	topic     string
	partition int32
	offset    int64
}

type TopicPartionInfo struct {
	Topic   string
	Partion int
	Offset  uint64
}

type Input struct {
	sync.RWMutex
	inputDriver.PluginDriverInterface
	inputInfo        inputDriver.InputInfo
	status           inputDriver.StatusFlag
	err              error
	PluginStatusChan chan *inputDriver.PluginStatus
	eventID          uint64

	callback      inputDriver.Callback
	childCallBack func(message *sarama.ConsumerMessage) error

	brokerList []string

	kafkaGroup sarama.ConsumerGroup

	kafkaGroupCtx    context.Context
	kafkaGroupCancel context.CancelFunc

	topics map[string]map[string]bool

	positionMap map[string]map[int32]int64

	waitCommitOffset chan *waitCommitOffset
}

func NewInputPlugin() inputDriver.Driver {
	return &Input{}
}

func (c *Input) GetUriExample() (string, string) {
	return "127.0.0.1:9092", ""
}

func (c *Input) SetOption(inputInfo inputDriver.InputInfo, param map[string]interface{}) {
	c.inputInfo = inputInfo
	c.positionMap = make(map[string]map[int32]int64, 0)
	c.topics = make(map[string]map[string]bool, 0)
	c.brokerList = strings.Split(inputInfo.ConnectUri, ",")
	c.waitCommitOffset = make(chan *waitCommitOffset, 500)
}

func (c *Input) setStatus(status inputDriver.StatusFlag) {
	c.status = status
	if c.PluginStatusChan != nil {
		c.PluginStatusChan <- &inputDriver.PluginStatus{Status: status, Error: c.err}
	}
}

func (c *Input) Start(ch chan *inputDriver.PluginStatus) error {
	c.PluginStatusChan = ch
	c.setStatus(inputDriver.STARTING)
	return c.Start0()
}

func (c *Input) Start0() error {
	c.kafkaGroupCtx, c.kafkaGroupCancel = context.WithCancel(context.Background())
	for {
		c.Start1()
		select {
		case _ = <-c.kafkaGroupCtx.Done():
			break
		default:
			break
		}
	}
}

func (c *Input) Start1() error {
	config := sarama.NewConfig()
	config.Version, _ = sarama.ParseKafkaVersion("2.7.0")
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.AutoCommit.Enable = false
	c.kafkaGroup, c.err = sarama.NewConsumerGroup(c.brokerList, defaultKafkaGroupId, config)
	if c.err != nil {
		return c.err
	}
	c.GroupCosume()
	return nil
}

func (c *Input) GroupCosume() {
	defer c.kafkaGroup.Close()
	defer c.setStatus(inputDriver.STOPPED)
	topics, err := c.GetTopics()
	if err != nil {
		c.err = err
		return
	}
	if len(topics) == 0 {
		c.err = fmt.Errorf("topics is empty")
		return
	}
	c.setStatus(inputDriver.RUNNING)
	for {
		//关键代码
		//正常情况下：Consume()方法会一直阻塞
		//我测试发现，约30分钟左右，Consume()会返回，但没有error
		//无error的情况下，可以重复调用Consume()方法
		//当有error产生的时候，不确定Consume()是否能够继续完善的执行。
		//因此保险的办法是抛出panic，让进程重启。
		c.err = c.kafkaGroup.Consume(c.kafkaGroupCtx, topics, c)
		if c.err != nil {
			return
		}
	}
}

func (c *Input) Stop() error {
	c.setStatus(inputDriver.STOPPING)
	if c.kafkaGroupCancel != nil {
		c.kafkaGroupCancel()
	}
	c.kafkaGroupCancel = nil
	return nil
}

func (c *Input) Close() error {
	c.setStatus(inputDriver.CLOSED)
	return nil
}

func (c *Input) Kill() error {
	c.Stop()
	return nil
}

func (c *Input) GetLastPosition() *inputDriver.PluginPosition {
	return nil
}

func (c *Input) SetCallback(callback inputDriver.Callback) {
	c.callback = callback
}

func (c *Input) SetEventID(eventId uint64) error {
	c.eventID = eventId
	return nil
}

func (c *Input) getNextEventID() uint64 {
	atomic.AddUint64(&c.eventID, 1)
	return c.eventID
}
