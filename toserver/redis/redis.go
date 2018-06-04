package redis

import (
	"log"
	"github.com/jc3wish/Bifrost/toserver/driver"
	"github.com/garyburd/redigo/redis"
	"fmt"
	"encoding/json"
	"strings"
	"strconv"
	dataDriver "database/sql/driver"
)

func init(){
	driver.Register("redis",&MyConn{})
}

type MyConn struct {}

func (MyConn *MyConn) Open(uri string) driver.ConnFun{
	return newConn(uri)
}

func (MyConn *MyConn) GetTypeList() []string{
	return []string{"list","set"}
}

func (MyConn *MyConn) GetTypeAndRule() driver.TypeAndRule{
	return driver.TypeAndRule{
		DataTypeList:[]string{"json","string"},
		TypeList: map[string]driver.TypeRule{
			"list":driver.TypeRule{
				Key:"(.*)",
				Val:"(.*)",
			},
			"set":driver.TypeRule{
				Key:"(.*)",
				Val:"(.*)",
			},
		},
	}
}

func (MyConn *MyConn) GetUriExample() string{
	return "pwd@tcp(127.0.0.1:6379)/0"
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
	Uri    string
	pwd 	string
	database int
	network string
	status string
	conn   redis.Conn
	err    error
	expir  int
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

func (This *Conn) SetExpir(TimeOut int) {
	This.expir = TimeOut
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
	var c string
	switch data.(type){
		case string:
			c = data.(string)
		break
		case map[string]interface{}:
			cb, _ := json.Marshal(data.(map[string]interface{}))
			c = string(cb)
		break
		case map[string]dataDriver.Value:
			cb, _ := json.Marshal(data.(map[string]dataDriver.Value))
			c = string(cb)
		break
		default:
			c = fmt.Sprint(data)
		break
	}
	var err error
	if This.expir > 0{
		_, err = This.conn.Do("SET", key,c,"ex",This.expir)
	}else{
		_, err = This.conn.Do("SET", key,c)
	}
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
	_, err := This.conn.Do("DEL", key)
	if err != nil {
		This.err = err
		return false,err
	}
	return true,nil
}

func (This *Conn) SendToList(key string, data interface{}) (bool,error) {
	if This.err != nil {
		This.ReConnect()
	}
	var c string
	switch data.(type){
		case string:
			c = data.(string)
		break
		case map[string]interface{}:
			cb, _ := json.Marshal(data.(map[string]interface{}))
			c = string(cb)
		break
		case map[string]dataDriver.Value:
		cb, _ := json.Marshal(data.(map[string]dataDriver.Value))
		c = string(cb)
		break
		default:
			c = fmt.Sprint(data)
		break
	}
	_, err := This.conn.Do("SET", key,c)
	if err != nil {
		This.err = err
		return false,err
	}
	return true,nil
}