package kafka

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestInputCosume(t *testing.T) {
	convey.Convey("normal", t, func() {
		var dataList []*sarama.ConsumerMessage
		var callbackFunc = func(message *sarama.ConsumerMessage) error {
			dataList = append(dataList, message)
			return nil
		}
		c := NewInputKafka()
		c.kafkaGroupCtx, c.kafkaGroupCancel = context.WithCancel(context.Background())
		c.childCallBack = callbackFunc
		ws := c.InitInputCosume(5)
		for i := 0; i < 20; i++ {
			kafkaMsg := &sarama.ConsumerMessage{
				Topic:     fmt.Sprintf("topic_%d", i%5),
				Partition: 0,
			}
			c.SendToInputConsume(kafkaMsg)
		}
		c.CloseInputCosume()
		ws.Wait()
		convey.So(len(dataList), convey.ShouldEqual, 20)
	})
}

func TestInputKafka_CloseInputCosume(t *testing.T) {
	c := NewInputKafka()
	ws := c.InitInputCosume(0)
	c.CloseInputCosume()
	ws.Wait()
}

func TestInputKafka_CRC32KafkaMsgTopicAndPartition(t *testing.T) {
	convey.Convey("normal", t, func() {
		c := NewInputKafka()
		kafkaMsg1 := &sarama.ConsumerMessage{
			Topic:     fmt.Sprintf("topic_%d", 1),
			Partition: 0,
		}
		kafkaMsg1CRC32 := c.CRC32KafkaMsgTopicAndPartition(kafkaMsg1)
		kafkaMsg2 := &sarama.ConsumerMessage{
			Topic:     fmt.Sprintf("topic_%d", 2),
			Partition: 0,
		}
		kafkaMsg2CRC32 := c.CRC32KafkaMsgTopicAndPartition(kafkaMsg2)
		convey.So(kafkaMsg1CRC32, convey.ShouldNotEqual, kafkaMsg2CRC32)
	})
}

func TestInputKafka_SendToInputConsume(t *testing.T) {
	convey.Convey("send chan lock,and consume cancle", t, func() {
		c := NewInputKafka()
		c.kafkaGroupCtx, c.kafkaGroupCancel = context.WithCancel(context.Background())
		c.inputCosumeList = make([]chan *sarama.ConsumerMessage, 0)
		c.inputCosumeList = append(c.inputCosumeList, make(chan *sarama.ConsumerMessage, 1))
		go func() {
			<-time.After(1 * time.Second)
			c.kafkaGroupCancel()
		}()
		for i := 0; i < 3; i++ {
			kafkaMsg := &sarama.ConsumerMessage{
				Topic:     fmt.Sprintf("topic_%d", i%5),
				Partition: 0,
			}
			c.SendToInputConsume(kafkaMsg)
		}
	})
}
