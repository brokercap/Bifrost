package src

import (
	pluginDriver "github.com/jc3wish/Bifrost/plugin/driver"
	"sync"
	"encoding/json"
	"fmt"
)


const VERSION  = "v1.1.0"
const BIFROST_VERION = "v1.1.0"

var l sync.RWMutex

type dataTableStruct struct {
	lastEventype string
	Data map[string]string
}

type dataStruct struct {
	sync.RWMutex
	Data map[string][]*pluginDriver.PluginDataType
}

var dataMap map[string]*dataStruct

type PluginParam struct {
	Field 			map[string]string
	BactchSize      int16
}


func init(){
	pluginDriver.Register("clickhouse",&MyConn{},VERSION,BIFROST_VERION)
	dataMap = make(map[string]*dataStruct,0)
}

type MyConn struct {}

func (MyConn *MyConn) Open(uri string) pluginDriver.ConnFun{
	return newConn(uri)
}

func (MyConn *MyConn) CheckUri(uri string) error{
	return nil
}

func (MyConn *MyConn) GetUriExample() string{
	return "tcp://127.0.0.1:9000?username=&compress=true&debug=true"
}

type Conn struct {
	uri    	string
	status  string
	p		*PluginParam
}

func newConn(uri string) *Conn{
	f := &Conn{
		uri:uri,
	}
	return f
}

func (This *Conn) GetConnStatus() string {
	return This.status
}

func (This *Conn) SetConnStatus(status string) {
	This.status = status
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
	if param.BactchSize == 0{
		param.BactchSize = 200
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

func (This *Conn) Connect() bool {
	if _,ok:= dataMap[This.uri];!ok{
		dataMap[This.uri] = &dataStruct{
			Data: make(map[string][]*pluginDriver.PluginDataType,0),
		}
	}
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

func (This *Conn) sendToCacheList(data *pluginDriver.PluginDataType) {
	l.RLock()
	t := dataMap[This.uri]
	t.Lock()
	if _,ok := t.Data[data.TableName];!ok{
		t.Data[data.TableName] = make([]*pluginDriver.PluginDataType,0)
	}
	t.Data[data.TableName] = append(t.Data[data.TableName],data)
	t.Unlock()
	l.RUnlock()
}

func (This *Conn) Insert(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	This.sendToCacheList(data)
	return nil,nil
}

func (This *Conn) Update(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	This.sendToCacheList(data)
	return nil,nil
}

func (This *Conn) Del(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	This.sendToCacheList(data)
	return nil,nil
}

func (This *Conn) Query(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	return nil,nil
}

func (This *Conn) Commit() (*pluginDriver.PluginBinlog,error) {
	return nil, nil
}