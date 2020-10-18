package src

import (
	"encoding/json"
	"strings"
	"github.com/Shopify/sarama"
	"time"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"fmt"
)

const (
	RUNNING int8 = 1
	CLOSED int8 = 0
)

type Conn struct {
	Uri    			string
	status 			int8
	err    			error
	p      			*PluginParam
	producer		sarama.SyncProducer
}

type PluginParam struct {
	Topic 			string
	Key   			string
	BatchSize 		int
	Timeout			int
	RequiredAcks	sarama.RequiredAcks
	dataList		[]*sarama.ProducerMessage
	binlogList 		[]pluginDriver.PluginBinlog
	dataCurrentCount int
}

func newConn(uri string) *Conn{
	f := &Conn{
		Uri:uri,
	}
	f.Connect()
	return f
}

func (This *Conn) GetConnStatus() string {
	if This.status == RUNNING{
		return "running"
	}
	return "close"
}

func (This *Conn) SetConnStatus(status string) {
	if status == "running" {
		This.status = RUNNING
	}else{
		This.status = CLOSED
	}
}

func (This *Conn) newProducer() bool {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = This.p.RequiredAcks
	config.Producer.Timeout = time.Duration(This.p.Timeout) * time.Second
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	This.producer, This.err = sarama.NewSyncProducer(strings.Split(This.Uri, ","), config)
	if This.err == nil {
		This.status = RUNNING
		return true
	}else {

		return false
	}
}

func (This *Conn) Connect() bool {
	This.err = fmt.Errorf("no producer")
	This.status = CLOSED
	return true
}

func (This *Conn) GetParam(p interface{}) (interface{},error){
	s,err := json.Marshal(p)
	if err != nil{
		return nil,err
	}
	//var param *PluginParam
	param := &PluginParam{RequiredAcks: -1, Timeout: 10}
	err2 := json.Unmarshal(s,param)
	if err2 != nil{
		return nil,err2
	}
	if param.BatchSize <= 0{
		param.BatchSize = 1
	}
	if param.Timeout == 0 {
		param.Timeout = 10
	}
	if param.Timeout < 0 {
		param.Timeout = 0
	}
	switch param.RequiredAcks {
	case sarama.NoResponse,sarama.WaitForAll,sarama.WaitForLocal:
		break
	default:
		param.RequiredAcks = sarama.WaitForAll
		break
	}
	if len(param.dataList) == 0{
		param.dataList = make([]*sarama.ProducerMessage,0)
		param.binlogList = make([]pluginDriver.PluginBinlog,0)
		param.dataCurrentCount = 0
	}
	This.p = param
	return param,nil
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

func (This *Conn) ReConnect() bool {
	func(){
		defer func(){
			if err := recover();err != nil{
				return
			}
		}()
		if This.producer != nil {
			This.producer.Close()
		}
	}()
	r := This.newProducer()
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
	if This.producer != nil {
		func() {
			defer func() {
				if err := recover(); err != nil {
					return
				}
			}()
			This.producer.Close()
		}()
	}
	This.producer = nil
	This.status = CLOSED
	return true
}

func (This *Conn) Insert(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	return This.sendToList(data)
}

func (This *Conn) Update(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	return This.sendToList(data)
}

func (This *Conn) Del(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	return This.sendToList(data)
}

func (This *Conn) Query(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	return This.sendToList(data)
}


func (This *Conn) sendToList(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	Topic := fmt.Sprint(pluginDriver.TransfeResult(This.p.Topic,data,len(data.Rows)-1))
	msg := &sarama.ProducerMessage{}
	msg.Topic = Topic
	if This.p.Key != ""{
		Key := fmt.Sprint(pluginDriver.TransfeResult(This.p.Key,data,len(data.Rows)-1))
		msg.Key = sarama.StringEncoder(Key)
	}

	c,err := json.Marshal(data)
	if err != nil{
		return nil,err
	}
	msg.Value =  sarama.StringEncoder(c)
	if This.p.BatchSize > 1{
		This.p.dataList = append(This.p.dataList,msg)
		This.p.binlogList = append(This.p.binlogList,pluginDriver.PluginBinlog{data.BinlogFileNum,data.BinlogPosition})
		This.p.dataCurrentCount++
		if This.p.dataCurrentCount >= This.p.BatchSize{
			return This.sendToKafka()
		}
		return nil,nil
	}else{
		if This.status != RUNNING{
			This.ReConnect()
			if This.status != RUNNING{
				return nil,This.err
			}
		}
		_, _, err = This.producer.SendMessage(msg)
	}

	if err != nil{
		This.err = err
		This.status = CLOSED
		return nil,err
	}
	return nil,nil
}

func (This *Conn) sendToKafka() (binlog *pluginDriver.PluginBinlog, err error) {
	if This.status != RUNNING{
		This.ReConnect()
		if This.status != RUNNING{
			return nil,This.err
		}
	}
	if This.p.dataCurrentCount == 0{
		return nil,nil
	}
	if This.p.dataCurrentCount > This.p.BatchSize{
		list := This.p.dataList[:This.p.BatchSize]
		err = This.producer.SendMessages(list)
		if err == nil{
			This.p.dataList = This.p.dataList[This.p.BatchSize:]
			binlogInfo := This.p.binlogList[This.p.BatchSize]
			This.p.binlogList = This.p.binlogList[This.p.BatchSize:]
			This.p.dataCurrentCount -= This.p.BatchSize
			binlog = &binlogInfo
		}
	}else{
		err = This.producer.SendMessages(This.p.dataList)
		if err == nil{
			This.p.dataList = make([]*sarama.ProducerMessage,0)
			This.p.dataCurrentCount = 0
			binlogInfo := This.p.binlogList[len(This.p.binlogList)-1]
			This.p.binlogList = make([]pluginDriver.PluginBinlog,0)
			binlog = &binlogInfo
		}
	}
	return binlog,err
}

func (This *Conn) Commit() (*pluginDriver.PluginBinlog,error){
	var binlog *pluginDriver.PluginBinlog
	for{
		binlogInfo,err := This.sendToKafka()
		if err != nil{
			return binlog,err
		}
		if err == nil && binlogInfo == nil{
			break
		}
		binlog = binlogInfo
	}
	return binlog,nil
}