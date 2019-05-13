package src

import (
	"github.com/brokercap/Bifrost/plugin/driver"
	"github.com/bradfitz/gomemcache/memcache"
	"fmt"
	"encoding/json"
)

const VERSION  = "v1.1.0"
const BIFROST_VERION = "v1.1.0"

func init(){
	driver.Register("memcache",&MyConn{},VERSION,BIFROST_VERION)
}

type MyConn struct {}

func (MyConn *MyConn) Open(uri string) driver.ConnFun{
	return newConn(uri)
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
	uri    		string
	status 		string
	conn   		*memcache.Client
	err    		error
	p 			*PluginParam
}

type PluginParam struct {
	KeyConfig 		string
	Expir 			int32
	DataType 		string
	ValConfig 		string
}


func newConn(uri string) *Conn{
	f := &Conn{
		uri:uri,
	}
	f.Connect()
	return f
}

func (This *Conn) GetParam(p interface{}) (*PluginParam,error){
	s,err := json.Marshal(p)
	if err != nil{
		return nil,err
	}
	var param PluginParam
	err2 := json.Unmarshal(s,&param)
	if err2 != nil{
		return nil,err2
	}
	This.p = &param
	return &param,nil
}

func (This *Conn) SetParam(p interface{}) (interface{},error){
	if p == nil{
		return nil,fmt.Errorf("param is nil")
	}
	switch p.(type) {
	case *PluginParam:
		This.p = p.(*PluginParam)
		return p,nil
	default:
		return This.GetParam(p)
	}
}

func (This *Conn) GetConnStatus() string {
	return This.status
}

func (This *Conn) SetConnStatus(status string) {
	This.status = status
}

func (This *Conn) Connect() bool {
	This.conn = memcache.New(This.uri)
	if This.conn == nil {
		This.err = fmt.Errorf("memcache New failed",This.uri)
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

func (This *Conn) getKeyVal(data *driver.PluginDataType,index int) string {
	return driver.TransfeResult(This.p.KeyConfig,data,index)
}

func (This *Conn) getVal(data *driver.PluginDataType,index int) string {
	return driver.TransfeResult(This.p.ValConfig,data,index)
}

func (This *Conn) Insert(data *driver.PluginDataType) (*driver.PluginBinlog,error) {
	return This.Update(data)
}

func (This *Conn) Update(data *driver.PluginDataType) (*driver.PluginBinlog,error) {
	if This.err != nil {
		This.ReConnect()
	}
	index := len(data.Rows)-1
	Key := This.getKeyVal(data,index)
	var Val []byte
	if This.p.ValConfig != ""{
		Val = []byte(This.getVal(data,index))
	}else{
		p := data.Rows[index]
		Val, _ = json.Marshal(p)
	}
	var err error
	err = This.conn.Set(&memcache.Item{Key: Key, Value: Val,Expiration:This.p.Expir})

	if err != nil {
		This.err = err
		return nil,err
	}
	return &driver.PluginBinlog{data.BinlogFileNum,data.BinlogPosition},nil
}

func (This *Conn) Del(data *driver.PluginDataType) (*driver.PluginBinlog,error) {
	if This.err != nil {
		This.ReConnect()
	}
	Key := This.getKeyVal(data, 0)
	var err error
	err = This.conn.Delete(Key)
	if err != nil {
		This.err = err
		return nil,err
	}
	return &driver.PluginBinlog{data.BinlogFileNum,data.BinlogPosition},nil
}

func (This *Conn) Query(data *driver.PluginDataType) (*driver.PluginBinlog,error) {
	return &driver.PluginBinlog{data.BinlogFileNum,data.BinlogPosition},nil
}

func (This *Conn) Commit() (*driver.PluginBinlog,error){
	return nil,nil
}