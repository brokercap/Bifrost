package src

import (
	pluginDriver "github.com/jc3wish/Bifrost/plugin/driver"
	"sync"
	"encoding/json"
	"fmt"
	"github.com/pingcap/tidb/meta"
	"database/sql"
)


const VERSION  = "v1.1.0"
const BIFROST_VERION = "v1.1.0"

var l sync.RWMutex

type dataTableStruct struct {
	lastEventype 	string
	MetaMap			map[string]string //字段类型
	Data 			[]*pluginDriver.PluginDataType
}

type dataStruct struct {
	sync.RWMutex
	Data map[string]*dataTableStruct
}

var dataMap map[string]*dataStruct

type PluginParam struct {
	Field 			map[string]string
	BactchSize      int16
	CkSchema		string
	CkTable			string
	ckDatakey		string
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
	conn    *clickhouseDB
	err 	error
}

func newConn(uri string) *Conn{
	f := &Conn{
		uri:uri,
	}
	f.conn = newClickHouseDBConn(uri)
	f.err = f.conn.err
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
	param.ckDatakey = param.CkSchema+"-"+param.CkTable
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
			Data: make(map[string]*dataTableStruct,0),
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
	if _,ok := t.Data[This.p.ckDatakey];!ok{
		t.Data[This.p.ckDatakey] = &dataTableStruct{
			lastEventype:"",
			MetaMap:make(map[string]string,0),
			Data:make([]*pluginDriver.PluginDataType,0),
		}
	}
	t.Data[This.p.ckDatakey].lastEventype = data.EventType
	t.Data[This.p.ckDatakey].Data = append(t.Data[This.p.ckDatakey].Data,data)
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
	t := dataMap[This.uri]
	t.Lock()
	defer t.Unlock()
	if _,ok := t.Data[This.p.ckDatakey];!ok{
		return nil,nil
	}
	n := len(t.Data[This.p.ckDatakey].Data)
	if n > int(This.p.BactchSize){
		n = int(This.p.BactchSize)
	}
	list := t.Data[This.p.ckDatakey].Data[:n]

	var stmtMap = make(map[string]*sql.Stmt)

	var getStmt = func(Type string,tx *sql.Tx) *sql.Stmt{
		if _,ok := stmtMap[Type];ok{
			return stmtMap[Type]
		}
		switch Type {
		case "insert":
			stmtMap[Type],_ = tx.Prepare("INSERT INTO example (country_code, os_id, browser_id, categories, action_day, action_time) VALUES (?, ?, ?, ?, ?, ?)")
				break
		}

		return stmtMap[Type]
	}

	tx,err:=This.conn.conn.Begin()
	if err != nil{
		This.err = err
		This.conn.err = err
		return nil,nil
	}
	for _,data := range list{
		getStmt(data.EventType,tx)
	}
	tx.Commit()
	return nil, nil
}