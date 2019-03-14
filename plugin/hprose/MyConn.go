package hprose


import (
	"github.com/jc3wish/Bifrost/plugin/driver"
)

func init(){
	driver.Register("hprose",&MyConn{})
}

type MyConn struct {}

func (MyConn *MyConn) Open(uri string) driver.ConnFun{
	return newConn(uri)
}

func (MyConn *MyConn) GetUriExample() string{
	return "http://127.0.0.1:61613 or tcp4://127.0.0.1:4321/"
}
func (MyConn *MyConn) CheckUri(uri string) error{
	c:= newConn(uri)
	return c.CheckUri()
}

