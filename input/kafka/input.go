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
	"regexp"
	"sync"
	"sync/atomic"
	"time"

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

type InputKafka struct {
	sync.RWMutex
	inputDriver.PluginDriverInterface
	inputInfo        inputDriver.InputInfo
	status           inputDriver.StatusFlag
	err              error
	PluginStatusChan chan *inputDriver.PluginStatus
	eventID          uint64

	config *Config

	callback      inputDriver.Callback
	childCallBack func(message *sarama.ConsumerMessage) error

	kafkaGroup sarama.ConsumerGroup

	kafkaGroupCtx    context.Context
	kafkaGroupCancel context.CancelFunc

	consumeClaimCtx    context.Context
	consumeClaimCancle context.CancelFunc

	inputCosumeList []chan *sarama.ConsumerMessage

	topics map[string]map[string]bool

	positionMap map[string]map[int32]int64

	waitCommitOffset chan *inputDriver.PluginPosition

	lastOffset *inputDriver.PluginPosition
}

func NewInputKafka() *InputKafka {
	c := &InputKafka{}
	c.Init()
	return c
}

func (c *InputKafka) GetUriExample() (string, string) {
	notesHtml := `
	<p><span class="help-block m-b-none">127.0.0.1:9092</span></p>
	<p><span class="help-block m-b-none">127.0.0.1:9092,192.168.1.10</span></p>
	<p><span class="help-block m-b-none">127.0.0.1:9092,192.168.1.10/topic1,topic2?from.beginning=false</span></p>
	<p><span class="help-block m-b-none">string_kafka: 将kafka中整条数据作为一个key进行处理</span></p>
	<p><span class="help-block m-b-none">canal_kafka: 支持将kafka中canal的json数据进行解析</span></p>
	<p><span class="help-block m-b-none">bifrost_kafka: 支持解析bifrost写入到kafka中的json数据</span></p>
	<p><span class="help-block m-b-none" style="color:#F00">如果新增了 Topic 等同步，需要手工进行对数据源 进行 Start 一次</span></p>
`
	return "127.0.0.1:9092,192.168.1.10/[topic_name1,topic_name2]][?client.id=&from.beginning=false]", notesHtml
}

func (c *InputKafka) Init() {
	c.positionMap = make(map[string]map[int32]int64, 0)
	c.topics = make(map[string]map[string]bool, 0)
	c.waitCommitOffset = make(chan *inputDriver.PluginPosition, 500)
}

func (c *InputKafka) SetOption(inputInfo inputDriver.InputInfo, param map[string]interface{}) {
	dsnMap := ParseDSN(inputInfo.ConnectUri)
	c.config, c.err = getKafkaConnectConfig(dsnMap)
	c.config.ParamMap = dsnMap
	c.inputInfo = inputInfo
}

func (c *InputKafka) setStatus(status inputDriver.StatusFlag) {
	c.status = status
	switch status {
	case inputDriver.CLOSED:
		c.err = fmt.Errorf("")
		break
	}
	if c.PluginStatusChan != nil {
		c.PluginStatusChan <- &inputDriver.PluginStatus{Status: status, Error: c.err}
	}
}

func (c *InputKafka) Start(ch chan *inputDriver.PluginStatus) error {
	c.PluginStatusChan = ch
	return c.Start0()
}

func (c *InputKafka) Start0() error {
	c.kafkaGroupCtx, c.kafkaGroupCancel = context.WithCancel(context.Background())
	c.InitInputCosume(c.config.CosumerCount)
	timeout := 2 * time.Second
	var timer *time.Timer
	for {
		c.setStatus(inputDriver.STARTING)
		c.Start1()
		timer = time.NewTimer(timeout)
		timer.Reset(timeout)
		select {
		case <-c.kafkaGroupCtx.Done():
			timer.Stop()
			return nil
		case <-time.NewTimer(timeout).C:
			timer.Stop()
			break
		}
	}
}

func (c *InputKafka) Start1() error {
	client, err := c.GetConn()
	if err != nil {
		c.err = err
		return err
	}
	c.kafkaGroup, c.err = sarama.NewConsumerGroupFromClient(c.GetCosumeGroupId(c.config.GroupId), client)
	if c.err != nil {
		return c.err
	}
	c.GroupCosume()
	return nil
}

func (c *InputKafka) GetCosumeGroupId(paramGroupId string) string {
	if paramGroupId != "" {
		return paramGroupId
	} else {
		// 只支持 英文 数字 _ 其他过滤
		reg := regexp.MustCompile(`[\W]{1,}`)
		return fmt.Sprintf("%s%s", defaultKafkaGroupIdPrefix, reg.ReplaceAllString(c.inputInfo.DbName, ""))
	}
}

func (c *InputKafka) GroupCosume() {
	defer c.kafkaGroup.Close()
	topics, err := c.GetTopics()
	if err != nil {
		c.err = err
		return
	}
	if len(topics) == 0 {
		c.err = fmt.Errorf("topics is empty")
		// 假如是找不到 topics 的情况下，直接进行close
		// 都没找到 topics 消费个啥
		c.Close()
		return
	}
	c.setStatus(inputDriver.RUNNING)
	defer c.setStatus(inputDriver.STOPPED)
	for {
		//关键代码
		//正常情况下：Consume()方法会一直阻塞
		//但是存在会自动退出，但没有error
		//所以无error的情况下，可以重复调用Consume()方法
		c.err = c.kafkaGroup.Consume(c.kafkaGroupCtx, topics, c)
		if c.err != nil {
			return
		}
	}
}

func (c *InputKafka) Stop() error {
	c.setStatus(inputDriver.STOPPING)
	if c.kafkaGroupCancel != nil {
		c.kafkaGroupCancel()
	}
	c.kafkaGroupCancel = nil
	if c.kafkaGroup != nil {
		c.kafkaGroup.Close()
	}
	return nil
}

func (c *InputKafka) Close() error {
	c.Stop()
	c.setStatus(inputDriver.CLOSED)
	return nil
}

func (c *InputKafka) Kill() error {
	c.Stop()
	c.Close()
	return nil
}

func (c *InputKafka) GetLastPosition() *inputDriver.PluginPosition {
	if c.lastOffset == nil {
		return nil
	}
	return c.lastOffset
}

func (c *InputKafka) SetCallback(callback inputDriver.Callback) {
	c.callback = callback
}

func (c *InputKafka) SetEventID(eventId uint64) error {
	c.eventID = eventId
	return nil
}

func (c *InputKafka) getNextEventID() uint64 {
	atomic.AddUint64(&c.eventID, 1)
	return c.eventID
}
