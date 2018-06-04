package memcache

import (
	"github.com/jc3wish/Bifrost/toserver/driver"
	"fmt"
	"encoding/json"
	dataDriver "database/sql/driver"
	"github.com/bradfitz/gomemcache/memcache"
)

func init(){
	driver.Register("memcache",&MyConn{})
}

type MyConn struct {}

func (MyConn *MyConn) Open(uri string) driver.ConnFun{
	return newConn(uri)
}

func (MyConn *MyConn) GetTypeList() []string{
	return []string{"set"}
}

func (MyConn *MyConn) GetTypeAndRule() driver.TypeAndRule{
	return driver.TypeAndRule{
		DataTypeList:[]string{"json","string"},
		TypeList: map[string]driver.TypeRule{
			"set":driver.TypeRule{
				Key:"(.*)",
				Val:"(.*)",
			},
		},
	}
}

func (MyConn *MyConn) GetUriExample() string{
	return "127.0.0.1:11211"
}

func (MyConn *MyConn) CheckUri(uri string) error{
	c:= newConn(uri)
	if c.err != nil{
		return c.err
	}
	c.Close()
	return nil
}


type Conn struct {
	Uri    string
	status string
	conn   *memcache.Client
	err    error
	expir  int32
}

func newConn(uri string) *Conn{
	f := &Conn{
		Uri:uri,
	}
	f.Connect()
	return f
}


func (This *Conn) GetConnStatus() string {
	return This.status
}

func (This *Conn) SetConnStatus(status string) {
	This.status = status
}

func (This *Conn) Connect() bool {
	This.conn = memcache.New(This.Uri)
	if This.conn == nil {
		This.err = fmt.Errorf("memcache New failed",This.Uri)
		return false
	}
	This.err = nil
	This.status = "running"
	return true
}

func (This *Conn) ReConnect() bool {
	defer func() {
		if err := recover();err !=nil{
			This.err = fmt.Errorf(fmt.Sprint(err))
		}
	}()
	This.Connect()
	return  true
}

func (This *Conn) HeartCheck() {
	return
}

func (This *Conn) Close() bool {
	return true
}

func (This *Conn) SetExpir(TimeOut int) {
	This.expir = int32(TimeOut)
}

func (This *Conn) SetMustBeSuccess(b bool) {
	return
}

func (This *Conn) Insert(key string, data interface{}) (bool,error) {
	return This.Update(key,data)
}

func (This *Conn) Update(key string, data interface{}) (bool,error) {
	if This.err != nil {
		This.ReConnect()
	}
	var c []byte
	switch data.(type){
		case string:
			c = []byte(data.(string))
		break
		case map[string]interface{}:
			c, _ = json.Marshal(data.(map[string]interface{}))
		break
		case map[string]dataDriver.Value:
			c, _ = json.Marshal(data.(map[string]dataDriver.Value))
		break
		default:
			c = []byte(fmt.Sprint(data))
		break
	}
	var err error
	This.conn.Set(&memcache.Item{Key: key, Value: c,Expiration:This.expir})
	if err != nil {
		This.err = err
		return false,err
	}
	return true,nil
}

func (This *Conn) Del(key string) (bool,error) {
	if This.err != nil {
		This.ReConnect()
	}
	err := This.conn.Delete(key)
	if err != nil {
		This.err = err
		return false,err
	}
	return true,nil
}

func (This *Conn) SendToList(key string, data interface{}) (bool,error) {
	return false,fmt.Errorf("memcache be not supported list")
}