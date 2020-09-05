package src


import (
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"strings"
	"github.com/Shopify/sarama"
)

const VERSION  = "v1.4.2"
const BIFROST_VERION = "v1.4.2"

func init(){
	pluginDriver.Register("kafka",&MyConn{},VERSION,BIFROST_VERION)
}
type MyConn struct {}

func (MyConn *MyConn) Open(uri string) pluginDriver.ConnFun{
	return newConn(uri)
}

func (MyConn *MyConn) GetUriExample() string{
	return "127.0.0.1:9092,127.0.0.1:9093"
}

func (MyConn *MyConn) CheckUri(uri string) error{
	producer, err := sarama.NewSyncProducer(strings.Split(uri, ","), nil)
	if err == nil {
		return err
	}else {
		producer.Close()
		return nil
	}
}
