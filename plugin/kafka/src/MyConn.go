package src


import (
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
)

const VERSION  = "v1.3.0"
const BIFROST_VERION = "v1.3.0"

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
	c:= newConn(uri)
	if c.err != nil{
		return c.err
	}
	c.Close()
	return nil
}
