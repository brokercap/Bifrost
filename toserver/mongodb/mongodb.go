package mongodb

import (
	"github.com/Bifrost/toserver/driver"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"fmt"
	"strings"
	"encoding/json"
	dataDriver "database/sql/driver"
	"time"
)

func init(){
	driver.Register("mongodb",&MyConn{})
}

type MyConn struct {}


func (MyConn *MyConn) Open(uri string) driver.ConnFun{
	return newConn(uri)
}

func (MyConn *MyConn) GetDataTypeList() []string{
	return []string{"json","string"}
}

func (MyConn *MyConn) GetTypeAndRule() driver.TypeAndRule{
	return driver.TypeAndRule{
		DataTypeList:[]string{"json","string"},
		TypeList: map[string]driver.TypeRule{
			"set":driver.TypeRule{
				Key:"(.*)-(.*)-(.*)-(.*)",
				KeyExample:"{$SchemaName}-{$TableName}-PrimaryField-{$PrimaryField}",
				Val:"json",
			},
			"list":driver.TypeRule{
				Key:"(.*)-(.*)",
				KeyExample:"{$SchemaName}-{$TableName}",
				Val:"json",
			},
		},
		}
}

func (MyConn *MyConn) GetUriExample() string{
	return "[mongodb://][user:pass@]host1[:port1][,host2[:port2],...][/database][?options]"
}

func (MyConn *MyConn) CheckUri(uri string) error{
	return nil
}

type Conn struct {
	Uri    string
	status string
	conn   *mgo.Session
	err    error
	expir  int
	timeOutMap map[string]int
}

func newConn(uri string) *Conn{
	f := &Conn{
		Uri:uri,
		timeOutMap:make(map[string]int,0),
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

func (This *Conn) SetExpir(TimeOut int) {
	return
	This.expir = TimeOut
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

func (This *Conn) Insert(key string, data interface{}) (bool,error) {
	s := strings.Split(key,"-")
	if len(s) != 4{
		return false,fmt.Errorf("key must be {$DB}-{$Table}-{key}-{$keyVal}")
	}
	var m map[string]dataDriver.Value
	switch data.(type){
	case string:
		err := json.Unmarshal([]byte(data.(string)),&m)
		if err != nil{
			return false,err
		}
	case map[string]dataDriver.Value:
		m = data.(map[string]dataDriver.Value)
	default:
		return false,fmt.Errorf("data.(type) must be map[string]dataDriver.Value")
	}
	m["createdAt"] = time.Now().Format("2006-01-02 15:04:05")
	m[s[2]] = s[3]
	c := This.conn.DB(s[0]).C(s[1])
	k := make(bson.M,1)
	k[s[2]] = s[3]
	_,err:=c.Upsert(k,data)
	if err !=nil{
		return false,err
	}
	return true,nil
}

func (This *Conn) Update(key string, data interface{}) (bool,error) {
	return This.Insert(key,data)
}

func (This *Conn) Del(key string) (bool,error) {
	s := strings.Split(key,"-")
	if len(s) != 4{
		return false,fmt.Errorf("key must be {$DB}-{$Table}-{key}-{$keyVal}")
	}
	c := This.conn.DB(s[0]).C(s[1])
	k := make(bson.M,1)
	k[s[2]] = s[3]
	c.Remove(k)
	return true,nil
}

func (This *Conn) SendToList(key string, data interface{}) (bool,error) {
	s := strings.Split(key,"-")
	if len(s) != 2{
		return false,fmt.Errorf("key must be {$DB}-{$Table}")
	}
	var m map[string]dataDriver.Value
	switch data.(type){
	case string:
		err := json.Unmarshal([]byte(data.(string)),&m)
		if err != nil{
			return false,err
		}
	case map[string]dataDriver.Value:
		m = data.(map[string]dataDriver.Value)
		break
	default:
		return false,fmt.Errorf("data must be a map")
	}
	c := This.conn.DB(s[0]).C(s[1])
	err := c.Insert(data)
	if err != nil{
		return false,err
	}
	return true,nil
}