package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/smartystreets/goconvey/convey"
	"testing"
)

func Test_getKafkaConnectConfig(t *testing.T) {
	convey.Convey("normal", t, func() {
		url := "127.0.0.1:9092,192.168.1.10/topic1,topic2?from.beginning=true&version=2.7.1&consumer.count=3&skip.serialize.err=true"
		configMap := ParseDSN(url)
		c, err := getKafkaConnectConfig(configMap)
		version, _ := sarama.ParseKafkaVersion("2.7.1")
		convey.So(err, convey.ShouldBeNil)
		convey.So(c.CosumerCount, convey.ShouldEqual, 3)
		convey.So(c.SkipSerializeErr, convey.ShouldEqual, true)
		convey.So(len(c.Topics), convey.ShouldEqual, 2)
		convey.So(c.Topics[0], convey.ShouldEqual, "topic1")
		convey.So(c.Topics[1], convey.ShouldEqual, "topic2")
		convey.So(c.ParamConfig.Version.String(), convey.ShouldEqual, version.String())
		convey.So(c.ParamConfig.Consumer.Offsets.Initial, convey.ShouldEqual, sarama.OffsetOldest)
	})

	convey.Convey("normal default", t, func() {
		url := "127.0.0.1:9092,192.168.1.10/topic1,topic2"
		configMap := ParseDSN(url)
		c, err := getKafkaConnectConfig(configMap)
		convey.So(err, convey.ShouldBeNil)
		convey.So(c.CosumerCount, convey.ShouldEqual, 1)
		convey.So(c.SkipSerializeErr, convey.ShouldEqual, false)
		convey.So(len(c.Topics), convey.ShouldEqual, 2)
		convey.So(c.Topics[0], convey.ShouldEqual, "topic1")
		convey.So(c.Topics[1], convey.ShouldEqual, "topic2")
		convey.So(c.ParamConfig.Version.String(), convey.ShouldEqual, defaultKafkaVersion)
		convey.So(c.ParamConfig.Consumer.Offsets.Initial, convey.ShouldEqual, sarama.OffsetNewest)
	})
}
