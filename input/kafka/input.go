package kafka

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	inputDriver "github.com/brokercap/Bifrost/input/driver"
	"strconv"
	"strings"
)

type TopicPartionInfo struct {
	Topic string
	Partion int
	Offset uint64
}


type Input struct {
	inputDriver.PluginDriverInterface
	inputInfo inputDriver.InputInfo
	reslut  chan error
	status inputDriver.StatusFlag
	err		error
	PluginStatusChan chan *inputDriver.PluginStatus
	eventID uint64
	callback inputDriver.Callback
	childCallBack func(message *sarama.Message) error

	kafkaGroup sarama.ConsumerGroup

	kafkaGroupCtx context.Context

	topicPartionInfo *TopicPartionInfo
}

func NewInputPlugin () inputDriver.Driver  {
	return &Input{}
}

func (c *Input) GetUriExample() (string,string) {
	return "127.0.0.1:9092",""
}

func (c *Input) SetOption(inputInfo inputDriver.InputInfo,param map[string]interface{})  {
	c.inputInfo = inputInfo
	c.topicPartionInfo = &TopicPartionInfo{}
}

func (c *Input) Start(ch chan *inputDriver.PluginStatus) error {
	return c.Start0()
}

func (c *Input) Start0() error {
	tmpArr := strings.Split(c.inputInfo.GTID,":")
	topic := tmpArr[0]
	if topic == "" {
		return fmt.Errorf("topic is empty , example gitd: topic:partion:offset")
	}
	var partion int
	if len(tmpArr) >= 2 {
		partion, c.err = strconv.Atoi(tmpArr[1])
		if c.err != nil {
			return c.err
		}
	}
	var offset uint64
	if len(tmpArr) >= 3 {
		offset, c.err = strconv.ParseUint(tmpArr[2], 10, 64)
		if c.err != nil {
			return c.err
		}
	}
	c.topicPartionInfo.Topic = topic
	c.topicPartionInfo.Partion = partion
	c.topicPartionInfo.Offset = offset

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	kafkaAddr := strings.Split(c.inputInfo.ConnectUri,",")
	groupId := fmt.Sprintf("bifrost_group_%s_%d",topic,partion)
	c.kafkaGroup, c.err = sarama.NewConsumerGroup(kafkaAddr, groupId, config)
	if c.err != nil {
		return c.err
	}
	go c.GroupCosume()
	return nil
}

func (c *Input) GroupCosume() {
	defer c.kafkaGroup.Close()
	for {
		//关键代码
		//正常情况下：Consume()方法会一直阻塞
		//我测试发现，约30分钟左右，Consume()会返回，但没有error
		//无error的情况下，可以重复调用Consume()方法
		//当有error产生的时候，不确定Consume()是否能够继续完善的执行。
		//因此保险的办法是抛出panic，让进程重启。
		topic := strings.Split(c.inputInfo.GTID, ":")[0]
		c.err = c.kafkaGroup.Consume(c.kafkaGroupCtx, []string{topic}, c)
		if c.err != nil {
			return
		}
	}
}

func (c *Input) monitorDump() (r bool) {
	defer func() {
		if err := recover(); err != nil {
			// 上一层 PluginStatusChan 在进程退出之前会被关闭，这里需要无视异常情况
		}
	}()
	for {
		select {
		case v := <- c.reslut:
			if v == nil {
				return
			}
			switch v.Error() {
			case "stop":
				c.status = inputDriver.STOPPED
				break
			case "running":
				c.status = inputDriver.RUNNING
				c.err = nil
				break
			case "starting":
				c.status = inputDriver.STARTING
				break
			case "close":
				c.status = inputDriver.CLOSED
				c.err = nil
				return
			default:
				c.status = inputDriver.CLOSED
				c.err = v
				break
			}
			break
		}
		c.PluginStatusChan <- &inputDriver.PluginStatus{Status:c.status , Error: c.err}

	}
	return true
}

func (c *Input) Stop() error {
	return nil
}

func (c *Input) Close() error {
	return nil
}

func (c *Input) Kill() error {
	return nil
}

func (c *Input) GetLastPosition() *inputDriver.PluginPosition {
	return nil
}

func (c *Input) SetEventID(eventId uint64) error {
	c.eventID = eventId
	return nil
}

func (c *Input) SetCallback(callback inputDriver.Callback) {
	c.callback = callback
}