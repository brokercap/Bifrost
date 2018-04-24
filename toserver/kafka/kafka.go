package kafka

import (
	"fmt"
	"encoding/json"
	dataDriver "database/sql/driver"
	"strconv"
	"strings"
	"github.com/Shopify/sarama"
	"time"
)

type Conn struct {
	Uri    string
	status string
	conn   sarama.SyncProducer
	err    error
}

func newConn(uri string) *Conn{
	f := &Conn{
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
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Timeout = 10 * time.Second
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	This.conn, err = sarama.NewSyncProducer(strings.Split(This.Uri, ","), config)
	if err != nil{
		This.err = err
		This.status = "close"
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
	return
}

func (This *Conn) SetMustBeSuccess(b bool) {
	return
}

func (This *Conn) SendToList(key string, data interface{}) (bool,error) {
	if This.status != "running"{
		This.ReConnect()
		if This.status != "running"{
			return false,This.err
		}
	}
	s := strings.Split(key,"#")
	msg := &sarama.ProducerMessage{}
	switch len(s) {
	case 3:
		intv ,_ := strconv.Atoi(s[1])
		msg.Partition = int32(intv)
		msg.Key = sarama.StringEncoder(s[2])
		break
	case 2:
		intv ,_ := strconv.Atoi(s[1])
		msg.Partition = int32(intv)
		break
	case 1:
		break
	default:
		This.err = fmt.Errorf("key must topic[#Partition][#key]")
		This.status = "error"
		return false,fmt.Errorf("key must topic[#Partition][#key]")
	}
	msg.Topic = s[0]

	switch data.(type){
	case string:
		msg.Value = sarama.ByteEncoder(data.(string))
	case map[string]dataDriver.Value:
		c,_:=json.Marshal(data)
		msg.Value =  sarama.StringEncoder(c)
		break
	default:
		return false,fmt.Errorf("data must be a string or a map")
	}

	_, _, err := This.conn.SendMessage(msg)
	if err != nil{
		This.err = err
		This.status = "close"
		return false,err
	}
	return true,nil
}