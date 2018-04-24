package kafka


import (
	"github.com/Bifrost/toserver/driver"
)

func init(){
	driver.Register("kafka",&MyConn{})
}

type MyConn struct {}

func (MyConn *MyConn) Open(uri string) driver.ConnFun{
	return newConn(uri)
}


func (MyConn *MyConn) GetTypeAndRule() driver.TypeAndRule{
	return driver.TypeAndRule{
		DataTypeList:[]string{"json","string"},
		TypeList: map[string]driver.TypeRule{
			"list":driver.TypeRule{
				Key:"(.*)(#(.*)(#(.*))?)?",
				Val:"(.*)",
				KeyExample:"topic#key#Partition",
			},
		},
	}
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
