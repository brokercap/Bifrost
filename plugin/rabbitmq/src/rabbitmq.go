package src

import (
	"encoding/json"
	"fmt"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"github.com/streadway/amqp"
	"log"
	"strconv"
)

const VERSION = "v1.6.0"
const BIFROST_VERION = "v1.0.0"

func init() {
	pluginDriver.Register("rabbitmq", NewConn, VERSION, BIFROST_VERION)
}

type Conn struct {
	pluginDriver.PluginDriverInterface
	uri         *string
	status      string
	conn        *amqp.Connection
	ch          *amqp.Channel
	ch_nowait   *amqp.Channel
	confirmWait chan amqp.Confirmation
	p           *PluginParam
	err         error
	queueMap    map[string]bool
	exchangeMap map[string]bool
	bindMap     map[string]bool
}

type Queue struct {
	Name       string
	Durable    bool
	AutoDelete bool
}

type Exchange struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
}

type PluginParam struct {
	Queue              Queue
	Exchange           Exchange
	Confirm            bool
	Persistent         bool
	RoutingKey         string
	Expir              int
	Declare            bool
	expir              string
	deliveryMode       uint8
	BifrostFilterQuery bool // bifrost server 保留,是否过滤sql事件
}

func NewConn() pluginDriver.Driver {
	f := &Conn{status: "close"}
	return f
}

func (This *Conn) SetOption(uri *string, param map[string]interface{}) {
	This.uri = uri
	return
}

func (This *Conn) Open() error {
	This.Connect()
	return nil
}

func (This *Conn) GetUriExample() string {
	return "amqp://guest:guest@localhost:5672/MyVhost"
}

func (This *Conn) CheckUri() error {
	This.Connect()
	if This.err != nil {
		return This.err
	}
	This.Close()
	return nil
}

func (This *Conn) Connect() bool {
	var err error
	This.conn, err = amqp.Dial(*This.uri)
	if err != nil {
		This.err = err
		This.status = "close"
		return false
	}
	This.queueMap = make(map[string]bool, 0)
	This.exchangeMap = make(map[string]bool, 0)
	This.bindMap = make(map[string]bool, 0)
	This.err = nil
	This.status = "running"
	return true
}

func (This *Conn) getChannel(confirm bool) *amqp.Channel {
	if confirm == true {
		if This.ch == nil {
			This.ch, This.err = This.conn.Channel()
			if This.err != nil {
				This.ch = nil
				return nil
			}
			This.ch.Confirm(false)
			This.confirmWait = make(chan amqp.Confirmation, 1)
			This.ch.NotifyPublish(This.confirmWait)
		}
		return This.ch
	} else {
		if This.ch_nowait == nil {
			This.ch_nowait, This.err = This.conn.Channel()
			if This.err != nil {
				This.ch_nowait = nil
				return nil
			}
		}
		return This.ch_nowait
	}
}
func (This *Conn) ReConnect() bool {
	This.Close()
	r := This.Connect()
	if r == true {
		return true
	} else {
		return false
	}
}

func (This *Conn) Close() bool {
	if This.conn == nil {
		return true
	}
	func() {
		defer func() {
			if err := recover(); err != nil {
				log.Println("ReConnect recory:", err)
				return
			}
		}()
		if This.ch != nil {
			This.ch.Close()
			This.ch = nil
		}
		if This.ch_nowait != nil {
			This.ch_nowait.Close()
			This.ch_nowait = nil
		}
		This.conn.Close()
	}()
	This.conn = nil
	This.status = "close"
	This.err = fmt.Errorf("closed")
	return true
}

func (This *Conn) GetParam(p interface{}) (*PluginParam, error) {
	s, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	var param PluginParam
	err2 := json.Unmarshal(s, &param)
	if err2 != nil {
		return nil, err2
	}
	if param.Expir > 0 {
		param.expir = strconv.Itoa(param.Expir)
	}
	if param.Persistent == true {
		param.deliveryMode = 2
	} else {
		param.deliveryMode = 1
	}
	This.p = &param
	return &param, nil
}

func (This *Conn) SetParam(p interface{}) (interface{}, error) {
	if p == nil {
		return nil, fmt.Errorf("param is nil")
	}
	switch p.(type) {
	case *PluginParam:
		This.p = p.(*PluginParam)
		return p, nil
	default:
		return This.GetParam(p)
	}
}

func (This *Conn) Insert(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return This.sendToList(data)
}

func (This *Conn) Update(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return This.sendToList(data)
}

func (This *Conn) Del(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return This.sendToList(data)
}

func (This *Conn) Query(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return This.sendToList(data)
}

func (This *Conn) Commit(data *pluginDriver.PluginDataType, retry bool) (LastSuccessCommitData *pluginDriver.PluginDataType, ErrData *pluginDriver.PluginDataType, err error) {
	if This.p.BifrostFilterQuery {
		return data, nil, nil
	}
	LastSuccessCommitData, ErrData, err = This.sendToList(data)
	if err == nil {
		LastSuccessCommitData = data
	}
	return
}

func (This *Conn) Declare(Queue *string, Exchange *string, RoutingKey *string) error {
	ch := This.getChannel(This.p.Confirm)
	if ch == nil {
		This.status = "close"
		return This.err
	}
	if _, ok := This.queueMap[*Queue]; !ok {
		p := make(amqp.Table, 0)
		_, err := ch.QueueDeclare(*Queue, This.p.Queue.Durable, This.p.Queue.AutoDelete, false, false, p)
		if err != nil {
			return err
		}
		This.queueMap[*Queue] = true
	}

	if _, ok := This.exchangeMap[*Exchange]; !ok {
		p := make(amqp.Table, 0)
		err := ch.ExchangeDeclare(*Exchange, This.p.Exchange.Type, This.p.Exchange.Durable, false, false, false, p)
		if err != nil {
			return err
		}
		This.exchangeMap[*Exchange] = true
	}

	key := *Queue + "-" + *Exchange + "-" + *RoutingKey
	if _, ok := This.bindMap[key]; !ok {
		p := make(amqp.Table, 0)
		err := ch.QueueBind(*Queue, *RoutingKey, *Exchange, false, p)
		if err != nil {
			return err
		}
		This.bindMap[key] = true
	}
	return nil
}

func (This *Conn) sendToList(data *pluginDriver.PluginDataType) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	if This.status != "running" {
		This.ReConnect()
		if This.status != "running" {
			return nil, data, This.err
		}
	}
	c, err := json.Marshal(data)
	if err != nil {
		This.err = err
		return nil, data, err
	}
	var queuename string
	var exchange string
	var routingkey string
	index := len(data.Rows) - 1
	exchange = fmt.Sprint(pluginDriver.TransfeResult(This.p.Exchange.Name, data, index))
	routingkey = fmt.Sprint(pluginDriver.TransfeResult(This.p.RoutingKey, data, index))
	if This.p.Declare == true {
		queuename = fmt.Sprint(pluginDriver.TransfeResult(This.p.Queue.Name, data, index))
		if err := This.Declare(&queuename, &exchange, &routingkey); err != nil {
			return nil, data, err
		}
	}
	if This.p.Confirm == true {
		_, err = This.SendAndWait(&exchange, &routingkey, &c, This.p.deliveryMode)
	} else {
		_, err = This.SendAndNoWait(&exchange, &routingkey, &c, This.p.deliveryMode)
	}
	if err != nil {
		return nil, data, err
	}
	return nil, nil, nil
}

func (This *Conn) TimeOutCommit() (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return nil, nil, nil
}
