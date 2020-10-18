package src

import (
	"github.com/brokercap/Bifrost/plugin/driver"
	//"github.com/garyburd/redigo/redis"
	"github.com/go-redis/redis"
	"fmt"
	"encoding/json"
	"strings"
	"strconv"
	"time"
)

const VERSION  = "v1.3.0"
const BIFROST_VERION = "v1.3.0"

func init(){
	driver.Register("redis",&MyConn{},VERSION,BIFROST_VERION)
}

type MyConn struct {}

func (MyConn *MyConn) Open(uri string) driver.ConnFun{
	return newConn(uri)
}

func (MyConn *MyConn) GetUriExample() string{
	return "pwd@tcp(127.0.0.1:6379)/0 or 127.0.0.1:6379 or pwd@tcp(127.0.0.1:6379,127.0.0.1:6380)/0 or 127.0.0.1:6379,127.0.0.1:6380"
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
	conn   		redis.UniversalClient
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
	if This.database < 0 || This.database >16{
		This.err = fmt.Errorf("database must be in 0 and 16")
		return false
	}
	if This.network != "tcp" {
		This.err = fmt.Errorf("network must be tcp")
		return false
	}

	universalClient := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:    strings.SplitN(This.Uri, ",", -1),
		Password: This.pwd,
		DB:       This.database,
		PoolSize: 4096,
	})

	_, This.err = universalClient.Ping().Result()
	if This.err != nil {
		This.status = ""
		return false
	}
	This.conn = universalClient
	if This.conn == nil{
		This.status = ""
		return false
	}else{
		This.err = nil
		return true
	}
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
	return fmt.Sprint(driver.TransfeResult(This.p.KeyConfig,data,index))
}

func (This *Conn) getVal(data *driver.PluginDataType,index int) string {
	return fmt.Sprint(driver.TransfeResult(This.p.ValConfig,data,index))
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
			err =This.conn.Set(Key, This.getVal(data,index), time.Duration(This.p.Expir) * time.Second).Err()
		}else{
			vbyte, _ := json.Marshal(data.Rows[index])
			err =This.conn.Set(Key, string(vbyte), time.Duration(This.p.Expir) * time.Second).Err()
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
		err = This.conn.Del(Key).Err()
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
	err =This.conn.LPush(Key, Val).Err()

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