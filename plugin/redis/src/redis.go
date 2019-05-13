package src

import (
	"log"
	"github.com/brokercap/Bifrost/plugin/driver"
	"github.com/garyburd/redigo/redis"
	"fmt"
	"encoding/json"
	"strings"
	"strconv"
)

const VERSION  = "v1.1.0"
const BIFROST_VERION = "v1.1.0"

func init(){
	driver.Register("redis",&MyConn{},VERSION,BIFROST_VERION)
}

type MyConn struct {}

func (MyConn *MyConn) Open(uri string) driver.ConnFun{
	return newConn(uri)
}

func (MyConn *MyConn) GetUriExample() string{
	return "pwd@tcp(127.0.0.1:6379)/0 or 127.0.0.1:6379"
}

func (MyConn *MyConn) CheckUri(uri string) error{
	c:= newConn(uri)
	if c.err != nil{
		return c.err
	}
	c.Close()
	return nil
}

func getUriParam(uri string)(pwd string, network string, url string, database int){
	i := strings.IndexAny(uri, "@")
	pwd = ""
	if i > 0{
		pwd = uri[0:i]
		url = uri[i+1:]
	}else{
		url = uri
	}
	i = strings.IndexAny(url, "/")
	if i > 0 {
		databaseString := url[i+1:]
		intv,err:=strconv.Atoi(databaseString)
		if err != nil{
			database = -1
		}
		database = intv
		url = url[0:i]
	}else{
		database = 0
	}
	i = strings.IndexAny(url, "(")
	if i > 0{
		network = url[0:i]
		url = url[i+1:len(url)-1]
	}else{
		network = "tcp"
	}
	return
}

type Conn struct {
	Uri    		string
	pwd 		string
	database 	int
	network 	string
	status 		string
	conn   		redis.Conn
	err    		error
	p 			*PluginParam
}

type PluginParam struct {
	KeyConfig 		string
	Expir 			int
	DataType 		string
	ValConfig 		string
	Type 			string
}


func newConn(uri string) *Conn{
	pwd,network,uri,database := getUriParam(uri)
	f := &Conn{
		pwd:pwd,
		network:network,
		database:database,
		Uri:uri,
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
	var err error
	if This.database < 0 || This.database >16{
		This.err = fmt.Errorf("database must be in 0 and 16")
		return false
	}
	if This.network != "tcp" && This.network != "udp"{
		This.err = fmt.Errorf("network must be tcp or udp")
		return false
	}
	if This.pwd != ""{
		This.conn, err = redis.Dial(This.network, This.Uri,redis.DialPassword(This.pwd))
	}else{
		This.conn, err = redis.Dial(This.network, This.Uri)
	}
	if err != nil {
		log.Println("Connect to redis error", err)
		This.err = err
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
	var err error
	switch This.p.Type {
	case "set":
		if This.p.ValConfig != ""{
			if This.p.Expir > 0{
				_, err = This.conn.Do("SET", Key,This.getVal(data,index),"ex",This.p.Expir)
			}else{
				_, err = This.conn.Do("SET", Key,This.getVal(data,index))
			}
		}else{
			vbyte, _ := json.Marshal(data.Rows[index])
			if This.p.Expir > 0{
				_, err = This.conn.Do("SET", Key,string(vbyte),"ex",This.p.Expir)
			}else{
				_, err = This.conn.Do("SET", Key,string(vbyte))
			}
		}
		break
	case "list":
		_,err = This.SendToList(Key,data)
		break
	default:
		err = fmt.Errorf(This.p.Type+ " not in(set,list)")
		break
	}

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
	switch This.p.Type {
	case "set":
		_, err = This.conn.Do("DEL", Key)
		break
	case "list":
		_,err = This.SendToList(Key,data)
		break
	default:
		err = fmt.Errorf(This.p.Type+ " not in(set,list)")
	}
	if err != nil {
		This.err = err
		return nil,err
	}
	return &driver.PluginBinlog{data.BinlogFileNum,data.BinlogPosition},nil
}

func (This *Conn) SendToList(Key string, data *driver.PluginDataType) (*driver.PluginBinlog,error) {
	var Val string
	var err error
	if This.p.ValConfig != ""{
		Val = This.getVal(data,0)
	}else{
		if This.p.ValConfig != ""{
			Val = This.getVal(data,0)
		}else{
			c,err := json.Marshal(data)
			if err != nil{
				return nil,err
			}
			Val = string(c)
		}
	}
	_, err = This.conn.Do("LPUSH", Key,Val)
	if err != nil {
		return nil,err
	}
	return &driver.PluginBinlog{data.BinlogFileNum,data.BinlogPosition},nil
}

func (This *Conn) Query(data *driver.PluginDataType) (*driver.PluginBinlog,error) {
	if This.p.Type == "list"{
		Key := This.getKeyVal(data, 0)
		return This.SendToList(Key,data)
	}
	return &driver.PluginBinlog{data.BinlogFileNum,data.BinlogPosition},nil
}

func (This *Conn) Commit() (*driver.PluginBinlog,error){
	return nil,nil
}