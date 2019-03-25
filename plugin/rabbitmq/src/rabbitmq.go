package src

import (
	pluginDriver "github.com/jc3wish/Bifrost/plugin/driver"
	"github.com/streadway/amqp"
	"strconv"
	"encoding/json"
)

const VERSION  = "v1.1.0"
const BIFROST_VERION = "v1.1.0"

func init(){
	pluginDriver.Register("rabbitmq",&MyConn{},VERSION,BIFROST_VERION)
}

type MyConn struct {}


func (MyConn *MyConn) Open(uri string) pluginDriver.ConnFun{
	return newConn(uri)
}

func (MyConn *MyConn) GetUriExample() string{
	return "amqp://guest:guest@localhost:5672/MyVhost"
}

func (MyConn *MyConn) CheckUri(uri string) error{
	c := newConn(uri)
	if c.err != nil{
		return c.err
	}
	c.Close()
	return nil
}

type Conn struct {
	uri    			string
	status 			string
	conn   			*amqp.Connection
	ch 				*amqp.Channel
	confirmWait 	chan amqp.Confirmation
	p				PluginParam
	err				error
	expir			string
	deliveryMode	uint8
	queueMap		map[string]bool
	exchangeMap		map[string]bool
	bindMap			map[string]bool
}

func newConn(uri string) *Conn{
	f := &Conn{
		uri:uri,
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
	This.conn, err = amqp.Dial(This.uri)
	if err != nil{
		This.err = err
		This.status = "close"
		return false
	}
	This.ch,err = This.conn.Channel()
	if err != nil{
		This.err = err
		This.status = "close"
		This.conn.Close()
		return false
	}
	This.queueMap = make(map[string]bool,0)
	This.exchangeMap = make(map[string]bool,0)
	This.bindMap = make(map[string]bool,0)
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

type Queue struct {
	Name string
	Durable bool
	AutoDelete bool
}

type Exchange struct {
	Name string
	Type string
	Durable bool
	AutoDelete bool
}

type PluginParam struct {
	Queue 				Queue
	Exchange 			Exchange
	Confirm 			bool
	Persistent 			bool
	RoutingKey 			string
	Expir 				int
	Declare 			bool
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
	if This.p.Confirm == true{
		This.ch.Confirm(false)
		This.confirmWait = make(chan amqp.Confirmation,1)
		This.ch.NotifyPublish(This.confirmWait)
	}
	if This.p.Expir > 0{
		This.expir = strconv.Itoa(This.p.Expir)
	}
	if This.p.Persistent ==  true{
		This.deliveryMode = 2
	}else{
		This.deliveryMode = 1
	}
	return nil
}

func (This *Conn) Insert(data *pluginDriver.PluginDataType) (bool,error) {
	return This.sendToList(data)
}

func (This *Conn) Update(data *pluginDriver.PluginDataType) (bool,error) {
	return This.sendToList(data)
}

func (This *Conn) Del(data *pluginDriver.PluginDataType) (bool,error) {
	return This.sendToList(data)
}

func (This *Conn) Query(data *pluginDriver.PluginDataType) (bool,error) {
	return This.sendToList(data)
	return true,nil
}

func (This *Conn) Declare(Queue *string,Exchange *string,RoutingKey *string) (error){
	if _,ok := This.queueMap[*Queue]; !ok{
		p := make(amqp.Table,0)
		_,err := This.ch.QueueDeclare(*Queue,This.p.Queue.Durable,This.p.Queue.AutoDelete,false,false,p)
		if err != nil{
			return err
		}
		This.queueMap[*Queue] = true
	}

	if _,ok := This.exchangeMap[*Exchange]; !ok{
		p := make(amqp.Table,0)
		err := This.ch.ExchangeDeclare(*Exchange,This.p.Exchange.Type,This.p.Exchange.Durable,false,false,false,p)
		if err != nil{
			return err
		}
		This.exchangeMap[*Exchange] = true
	}

	key := *Queue+"-"+*Exchange+"-"+*RoutingKey
	if _,ok := This.bindMap[key]; !ok{
		p := make(amqp.Table,0)
		err := This.ch.QueueBind(*Queue,*RoutingKey,*Exchange,false,p)
		if err != nil{
			return err
		}
		This.bindMap[key] = true
	}
	return nil
}

func (This *Conn) sendToList(data *pluginDriver.PluginDataType) (bool,error) {
	if This.status != "running"{
		This.ReConnect()
		if This.status != "running"{
			return false,This.err
		}
	}
	c,err := json.Marshal(data)
	if err != nil{
		This.err = err
		return false,err
	}
	var queuename string
	var exchange string
	var routingkey string
	index := len(data.Rows)-1
	exchange = pluginDriver.TransfeResult(This.p.Exchange.Name,data,index)
	routingkey = pluginDriver.TransfeResult(This.p.RoutingKey,data,index)
	if This.p.Declare == true {
		queuename = pluginDriver.TransfeResult(This.p.Queue.Name, data, index)
		if err := This.Declare(&queuename,&exchange,&routingkey); err != nil{
			return false,err;
		}
	}

	if This.p.Confirm == true{
		return This.SendAndWait(&exchange,&routingkey,&c,&This.deliveryMode)
	}else{
		return This.SendAndNoWait(&exchange,&routingkey,&c,&This.deliveryMode)
	}
	return true,nil
}