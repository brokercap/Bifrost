package src

import (
	"encoding/json"
	"strings"
	"github.com/Shopify/sarama"
	"time"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"fmt"
)

type Conn struct {
	Uri    			string
	status 			string
	conn   			sarama.SyncProducer
	err    			error
	p      			*PluginParam
}

type PluginParam struct {
	Topic 			string
	Key   			string
	BatchSize 		int
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


func (This *Conn) GetParam(p interface{}) (interface{},error){
	s,err := json.Marshal(p)
	if err != nil{
		return nil,err
	}
	var param PluginParam
	err2 := json.Unmarshal(s,&param)
	if err2 != nil{
		return nil,err2
	}
	if param.BatchSize <= 0{
		param.BatchSize = 1
	}
	if len(param.dataList) == 0{
		param.dataList = make([]*sarama.ProducerMessage,0)
		param.binlogList = make([]pluginDriver.PluginBinlog,0)
		param.dataCurrentCount = 0
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
	if This.conn != nil {
		func() {
			defer func() {
				if err := recover(); err != nil {
					return
				}
			}()
			This.conn.Close()
		}()
	}
	This.conn = nil
	This.status = "close"
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
	if This.status != "running"{
		This.ReConnect()
		if This.status != "running"{
			return nil,This.err
		}
	}
	Topic := pluginDriver.TransfeResult(This.p.Topic,data,len(data.Rows)-1)
	msg := &sarama.ProducerMessage{}
	msg.Topic = Topic
	if This.p.Key != ""{
		Key := pluginDriver.TransfeResult(This.p.Key,data,len(data.Rows)-1)
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
		_, _, err = This.conn.SendMessage(msg)
	}

	if err != nil{
		This.err = err
		This.status = "close"
		return nil,err
	}
	return nil,nil
}

func (This *Conn) sendToKafka() (binlog *pluginDriver.PluginBinlog, err error) {
	if This.p.dataCurrentCount == 0{
		return nil,nil
	}
	if This.p.dataCurrentCount > This.p.BatchSize{
		list := This.p.dataList[:This.p.BatchSize]
		err = This.conn.SendMessages(list)
		if err == nil{
			This.p.dataList = This.p.dataList[This.p.BatchSize:]
			binlogInfo := This.p.binlogList[This.p.BatchSize]
			This.p.binlogList = This.p.binlogList[This.p.BatchSize:]
			This.p.dataCurrentCount -= This.p.BatchSize
			binlog = &binlogInfo
		}
	}else{
		err = This.conn.SendMessages(This.p.dataList)
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