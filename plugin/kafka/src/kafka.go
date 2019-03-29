package src

import (
	"encoding/json"
	"strings"
	"github.com/Shopify/sarama"
	"time"
	pluginDriver "github.com/jc3wish/Bifrost/plugin/driver"
)

type Conn struct {
	Uri    			string
	status 			string
	conn   			sarama.SyncProducer
	err    			error
	p      			PluginParam
	dataList 		[]*sarama.ProducerMessage
	binlogList 		[]pluginDriver.PluginBinlog
	dataCurrentCount int
}

type PluginParam struct {
	Topic 			string
	Key   			string
	BatchSize 		int
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
	if This.p.BatchSize <=0 {
		This.p.BatchSize = 1
	}
	if len(This.dataList) == 0{
		This.dataList = make([]*sarama.ProducerMessage,0)
		This.binlogList = make([]pluginDriver.PluginBinlog,0)
		This.dataCurrentCount = 0
	}
	return nil
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
		This.dataList = append(This.dataList,msg)
		This.binlogList = append(This.binlogList,pluginDriver.PluginBinlog{data.BinlogFileNum,data.BinlogPosition})
		This.dataCurrentCount++
		if This.dataCurrentCount >= This.p.BatchSize{
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
	if This.dataCurrentCount == 0{
		return nil,nil
	}
	if This.dataCurrentCount > This.p.BatchSize{
		list := This.dataList[:This.p.BatchSize]
		err = This.conn.SendMessages(list)
		if err == nil{
			This.dataList = This.dataList[This.p.BatchSize:]
			binlogInfo := This.binlogList[This.p.BatchSize]
			This.binlogList = This.binlogList[This.p.BatchSize:]
			This.dataCurrentCount -= This.p.BatchSize
			binlog = &binlogInfo
		}
	}else{
		err = This.conn.SendMessages(This.dataList)
		if err == nil{
			This.dataList = make([]*sarama.ProducerMessage,0)
			This.dataCurrentCount = 0
			binlogInfo := This.binlogList[len(This.binlogList)-1]
			This.binlogList = make([]pluginDriver.PluginBinlog,0)
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