package src

import (
	pluginDriver "github.com/jc3wish/Bifrost/plugin/driver"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"fmt"
	"encoding/json"
)

const VERSION  = "v1.1.0"
const BIFROST_VERION = "v1.1.0"

func init(){
	pluginDriver.Register("MongoDB",&MyConn{},VERSION,BIFROST_VERION)
}


type MyConn struct {}


func (MyConn *MyConn) Open(uri string) pluginDriver.ConnFun{
	return newConn(uri)
}


func (MyConn *MyConn) GetUriExample() string{
	return "[mongodb://][user:pass@]host1[:port1][,host2[:port2],...][/database][?options]"
}

func (MyConn *MyConn) CheckUri(uri string) error{
	c := newConn(uri)
	if c.status == "running"{
		c.Close()
		return nil
	}else{
		return c.err
	}
}

type Conn struct {
	Uri    string
	status string
	conn   *mgo.Session
	err    error
	expir  int
	timeOutMap map[string]int
	p		PluginParam
}


type PluginParam struct {
	SchemaName 			string
	TableName 			string
	PrimaryKey			string
}

func newConn(uri string) *Conn{
	f := &Conn{
		Uri:uri,
		timeOutMap:make(map[string]int,0),
	}
	f.Connect()
	return f
}

func (This *Conn) SetParam(p interface{}) error{
	s,err := json.Marshal(p)
	if err != nil{
		return err
	}
	var param PluginParam
	err2 := json.Unmarshal(s,&param)
	if err2 != nil{
		return err2
	}
	This.p = param
	return nil
}

func (This *Conn) GetConnStatus() string {
	return This.status
}

func (This *Conn) SetConnStatus(status string) {
	This.status = status
}

func (This *Conn) Connect() bool {
	var err error
	This.conn, err = mgo.Dial(This.Uri)
	if err != nil{
		This.err = err
		This.status = "close"
		return false
	}
	This.conn.SetMode(mgo.Monotonic, true)
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
	This.conn.Close()
	This.Connect()
	return  true
}

func (This *Conn) HeartCheck() {
	return
}

func (This *Conn) Close() bool {
	This.conn.Close()
	return true
}

func (This *Conn) createExpirIndex(s []string) {
	timeOutKey := s[0]+"-"+s[1]
	if _,ok:=This.timeOutMap[timeOutKey];ok{
		return
	}else{
		This.timeOutMap[timeOutKey] = This.expir
		This.conn.DB(s[0]).C(s[1])
	}
}

func (This *Conn) Insert(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	n := len(data.Rows)-1
	SchemaName := pluginDriver.TransfeResult(This.p.SchemaName, data, n)
	TableName := pluginDriver.TransfeResult(This.p.TableName, data, n)
	if This.p.PrimaryKey == ""{
		return nil,fmt.Errorf("PrimaryKey is empty")
	}
	if _,ok := data.Rows[n][This.p.PrimaryKey];!ok{
		return nil,fmt.Errorf("PrimaryKey "+ This.p.PrimaryKey +" is not exsit")
	}
	c := This.conn.DB(SchemaName).C(TableName)
	k := make(bson.M,1)
	k[This.p.PrimaryKey] = data.Rows[n][This.p.PrimaryKey]
	_,err:=c.Upsert(k,data.Rows[n])
	if err !=nil{
		return nil,err
	}
	return &pluginDriver.PluginBinlog{data.BinlogFileNum,data.BinlogPosition},nil
}

func (This *Conn) Update(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	return This.Insert(data)
}

func (This *Conn) Del(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	if This.p.PrimaryKey == ""{
		return nil,fmt.Errorf("PrimaryKey is empty")
	}
	if _,ok := data.Rows[0][This.p.PrimaryKey];!ok{
		return nil,fmt.Errorf("PrimaryKey "+ This.p.PrimaryKey +" is not exsit")
	}
	SchemaName := pluginDriver.TransfeResult(This.p.SchemaName, data, 0)
	TableName := pluginDriver.TransfeResult(This.p.TableName, data, 0)
	c := This.conn.DB(SchemaName).C(TableName)
	k := make(bson.M,1)
	k[This.p.PrimaryKey] = data.Rows[0][This.p.PrimaryKey]
	err := c.Remove(k)
	if err !=nil{
		return nil,err
	}
	return &pluginDriver.PluginBinlog{data.BinlogFileNum,data.BinlogPosition},nil
}

func (This *Conn) Query(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	return &pluginDriver.PluginBinlog{data.BinlogFileNum,data.BinlogPosition},nil
}

func (This *Conn) Commit() (*pluginDriver.PluginBinlog,error){
	return nil,nil
}
