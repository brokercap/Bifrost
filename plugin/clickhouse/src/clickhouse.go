package src

import (
	pluginDriver "github.com/jc3wish/Bifrost/plugin/driver"
	"sync"
	"encoding/json"
	"fmt"
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
	BactchSize      int
	CkSchema		string
	CkTable			string
	ckDatakey		string
	PriKey			string //主键字段
	replaceInto		bool  // 记录当前表是否有replace into操作
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
	c:= newConn(uri)
	if c.err != nil{
		return c.err
	}
	c.Close()
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
	f.Connect()
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
	param.ckDatakey = param.CkSchema+"."+param.CkTable
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
	This.conn = newClickHouseDBConn(This.uri)
	This.err = This.conn.err
	return true
}

func (This *Conn) ReConnect() bool {
	if This.conn != nil{
		defer func() {
			if err := recover();err !=nil{
				This.err = fmt.Errorf(fmt.Sprint(err))
			}
		}()
		This.conn.Close()
	}
	This.Connect()
	return  true
}

func (This *Conn) HeartCheck() {
	return
}

func (This *Conn) Close() bool {
	return true
}

func (This *Conn) sendToCacheList(data *pluginDriver.PluginDataType)  (*pluginDriver.PluginBinlog,error){
	var n int
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
	n = len(t.Data[This.p.ckDatakey].Data)
	t.Unlock()
	l.RUnlock()

	if This.p.BactchSize > n{
		return This.Commit()
	}
	return nil,nil
}

func (This *Conn) Insert(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	return This.sendToCacheList(data)
}

func (This *Conn) Update(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	return This.sendToCacheList(data)
}

func (This *Conn) Del(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	return This.sendToCacheList(data)
}

func (This *Conn) Query(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	return nil,nil
}

func (This *Conn) Commit() (*pluginDriver.PluginBinlog,error) {
	if This.err != nil {
		This.ReConnect()
	}
	if This.err != nil {
		return nil,This.err
	}
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
			fields := ""
			values := ""
			for k,_:= range This.p.Field{
				if fields == ""{
					fields = k
					values = "?"
				}else{
					fields = ","+fields
					values = ",?"
				}
			}
			stmtMap[Type],_ = tx.Prepare("INSERT INTO "+This.p.ckDatakey+" ("+fields+") VALUES ("+values+")")
			break
		case "delete":
			stmtMap[Type],_ = tx.Prepare("ALTER TABLE "+This.p.ckDatakey+" DELETE WHERE "+This.p.PriKey+"=?")
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

	//因为数据是有序写到list里的，里有 update,delete,insert，所以这里我们反向遍历
	//假如update之前，则说明，后面遍历的同一条数据都不需要再更新了
	//有一种比较糟糕的情况就是在源端replace into 操作，这个操作是先delete再insert操作，所以这种情况。假如自动发现了，则应该采用第二套方案处理

	if This.p.replaceInto == false {
		opMap := make(map[interface{}]string, 0)
		var checkOpMap = func(key interface{}, EvenType string) bool {
			if opMap[key] == "insert" && EvenType == "delete" {
				This.p.replaceInto = true
				return false
			}
			return true
		}
		for i := n - 1; i > 0; i-- {
			data := list[i]
			switch data.EventType {
			case "update":
				getStmt("delete", tx).Exec(data.Rows[0][This.p.PriKey])
				getStmt("insert", tx).Exec(data.Rows[1])
				opMap[data.Rows[1][This.p.PriKey]] = "update"
				break
			case "delete":
				if checkOpMap(data.Rows[0][This.p.PriKey], "delete") == false {
					tx.Rollback()
					goto Loop
					break
				}
				getStmt("delete", tx).Exec(data.Rows[0][This.p.PriKey])
				opMap[data.Rows[0][This.p.PriKey]] = "delete"
				break
			case "insert":
				getStmt("insert", tx).Exec(data.Rows[0])
				opMap[data.Rows[0][This.p.PriKey]] = "insert"
				break
			}
		}
	}

	Loop:
	if This.p.replaceInto == true{
		for _,data := range list{
			switch data.EventType {
			case "update":
				getStmt("delete", tx).Exec(data.Rows[0][This.p.PriKey])
				getStmt("insert", tx).Exec(data.Rows[1])
				break
			case "delete":
				getStmt("delete", tx).Exec(data.Rows[0][This.p.PriKey])
				break
			case "insert":
				getStmt("insert", tx).Exec(data.Rows[0])
				break
			}
		}
	}
	tx.Commit()

	if n == int(This.p.BactchSize){
		t.Data[This.p.ckDatakey].Data = make([]*pluginDriver.PluginDataType,0)
	}else{
		t.Data[This.p.ckDatakey].Data = t.Data[This.p.ckDatakey].Data[n+1:]
	}

	return &pluginDriver.PluginBinlog{list[n-1].BinlogFileNum,list[n-1].BinlogPosition}, nil
}