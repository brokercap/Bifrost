package src

import (
	"github.com/brokercap/Bifrost/plugin/driver"
	"fmt"
	"encoding/json"
	"strings"
)

const VERSION  = "v1.1.0"
const BIFROST_VERION = "v1.1.0"

func init(){
	driver.Register("TableCount",&MyConn{},VERSION,BIFROST_VERION)
}

type MyConn struct {}

func (MyConn *MyConn) Open(uri string) driver.ConnFun{
	return newConn(uri)
}

func (MyConn *MyConn) GetUriExample() string{
	return "TableCount"
}

func (MyConn *MyConn) CheckUri(uri string) error{
	return nil
}

type Conn struct {
	p 				*PluginParam
	eventCountBool	bool
}

type PluginParam struct {
	DbName 		string
}


func newConn(uri string) *Conn{
	f := &Conn{
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
	return "running"
}

func (This *Conn) SetConnStatus(status string) {

}

func (This *Conn) Connect() bool {
	return true
}

func (This *Conn) ReConnect() bool {
	return  true
}

func (This *Conn) HeartCheck() {
	return
}

func (This *Conn) Close() bool {
	return true
}

func (This *Conn) Insert(data *driver.PluginDataType) (*driver.PluginBinlog,error) {
	This.eventCountBool = true
	if data.BinlogPosition == 0{
		This.eventCountBool = false
	}
	AddCount(This.p.DbName,data.SchemaName,data.TableName,INSERT,len(data.Rows),This.eventCountBool)
	return &driver.PluginBinlog{data.BinlogFileNum,data.BinlogPosition},nil
}

func (This *Conn) Update(data *driver.PluginDataType) (*driver.PluginBinlog,error) {
	This.eventCountBool = true
	if data.BinlogPosition == 0{
		This.eventCountBool = false
	}
	AddCount(This.p.DbName,data.SchemaName,data.TableName,UPDATE,len(data.Rows)/2,This.eventCountBool)
	return &driver.PluginBinlog{data.BinlogFileNum,data.BinlogPosition},nil
}

func (This *Conn) Del(data *driver.PluginDataType) (*driver.PluginBinlog,error) {
	This.eventCountBool = true
	if data.BinlogPosition == 0{
		This.eventCountBool = false
	}
	AddCount(This.p.DbName,data.SchemaName,data.TableName,DELETE,len(data.Rows),This.eventCountBool)
	return &driver.PluginBinlog{data.BinlogFileNum,data.BinlogPosition},nil
}

func (This *Conn) Query(data *driver.PluginDataType) (*driver.PluginBinlog,error) {
	if len(data.Query) >= 11 && strings.ToUpper(data.Query[0:11]) == "ALTER TABLE" {
		AddCount(This.p.DbName, data.SchemaName, data.TableName, DDL, 0, true)
	}
	return &driver.PluginBinlog{data.BinlogFileNum,data.BinlogPosition},nil
}

func (This *Conn) Commit() (*driver.PluginBinlog,error){
	return nil,nil
}