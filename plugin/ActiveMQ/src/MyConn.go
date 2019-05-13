package src


import (
	"github.com/brokercap/Bifrost/plugin/driver"
)

const VERSION  = "v1.1.0"
const BIFROST_VERION = "v1.1.0"

func init(){
	driver.Register("ActiveMQ",&MyConn{},VERSION,BIFROST_VERION)
}

type MyConn struct {}

func (MyConn *MyConn) Open(uri string) driver.ConnFun{
	return newConn(uri)
}

func (MyConn *MyConn) GetUriExample() string{
	return "127.0.0.1:61613"
}

func (MyConn *MyConn) CheckUri(uri string) error{
	c:= newConn(uri)
	if c.err != nil{
		return c.err
	}
	c.Close()
	return nil
}
