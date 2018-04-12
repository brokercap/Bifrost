package rabbitmq

import (
	"github.com/Bifrost/toserver/driver"
	"github.com/streadway/amqp"
	"fmt"
	"strings"
	"encoding/json"
	dataDriver "database/sql/driver"
	"log"
	"strconv"
)

func init(){
	driver.Register("rabbitmq",&MyConn{})
}

type MyConn struct {}


func (MyConn *MyConn) Open(uri string) driver.ConnFun{
	return newConn(uri)
}

func (MyConn *MyConn) GetTypeAndRule() driver.TypeAndRule{
	return driver.TypeAndRule{
		DataTypeList:[]string{"json","string"},
		TypeList: map[string]driver.TypeRule{
			"list":driver.TypeRule{
				Key:"(.*)(-(.*)(-(0|1))?)?",
				Val:"json",
				KeyExample:"routingKey-amq.direct-0",
			},
		},
	}
}

func (MyConn *MyConn) GetUriExample() string{
	return "amqp://guest:guest@localhost:5672/MyVhost"
}

func (MyConn *MyConn) CheckUri(uri string) error{
	return nil
}

type Conn struct {
	Uri    string
	status string
	conn   *amqp.Connection
	ch 		*amqp.Channel
	err    error
	expir  string
}

func newConn(uri string) *Conn{
	f := &Conn{
		Uri:uri,
		expir:"",
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
	This.conn, err = amqp.Dial(This.Uri)
	if err != nil{
		log.Println("rabbitmq conn err:",err)
		This.err = err
		This.status = "close"
		return false
	}
	This.ch,err = This.conn.Channel()
	if err != nil{
		log.Println("rabbitmq Channel err:",err)
		This.err = err
		This.status = "close"
		This.conn.Close()
		return false
	}
	This.err = nil
	This.status = "running"
	return true
}

func (This *Conn) ReConnect() bool {
	func(){
		defer func(){
			if err := recover();err != nil{
				return
			}
		}()
		This.conn.Close()
		This.ch.Close()
	}()
	r := This.Connect()
	if r == true{
		return  true
	}else{
		return  false
	}
}

func (This *Conn) HeartCheck() {
	return
}

func (This *Conn) Close() bool {
	This.conn.Close()
	return true
}

func (This *Conn) Insert(key string, data interface{}) (bool,error) {
	return false,fmt.Errorf("not support insert")
}

func (This *Conn) Update(key string, data interface{}) (bool,error) {
	return false,fmt.Errorf("not support update")
}

func (This *Conn) Del(key string) (bool,error) {
	return false,fmt.Errorf("not support delete")
}

func (This *Conn) SetExpir(TimeOut int) {
	if TimeOut > 0 {
		This.expir = strconv.Itoa(TimeOut*1000)
	}else{
		This.expir = ""
	}
}

func (This *Conn) SendToList(key string, data interface{}) (bool,error) {
	if This.status != "running"{
		This.ReConnect()
		if This.status != "running"{
			return false,This.err
		}
	}
	s := strings.Split(key,"-")
	var DeliveryMode uint8 = 1
	var exchange string = "amq.default"
	var routingkey string
	switch len(s) {
	case 3:
		intv ,_ := strconv.Atoi(s[2])
		DeliveryMode = uint8(intv)
		exchange = s[1]
		routingkey = s[0]
		break
	case 2:
		routingkey = s[0]
		exchange = s[1]
		break
	case 1:
		routingkey = s[0]
		break
	default:
		This.err = fmt.Errorf("key must routingkey[-exchange][-DeliveryMode]")
		This.status = "error"
		return false,fmt.Errorf("key must be routingkey[-exchange][-DeliveryMode]")
	}
//	exchange = "amq.direct"
	var c []byte
	switch data.(type){
	case string:
		c = []byte(data.(string))
	case map[string]dataDriver.Value:
		c,_=json.Marshal(data)
		break
	default:
		return false,fmt.Errorf("data must be a string or a map")
	}
	err := This.ch.Publish(
		exchange,     // exchange
		routingkey, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			 ContentType: 	"text/plain",
					Body:   c,
			DeliveryMode:	DeliveryMode,
			  Expiration:	This.expir,
		})
	if err != nil{
		This.err = err
		This.status = "close"
		return false,err
	}
	return true,nil
}