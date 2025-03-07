/*
Copyright [2018] [jc3wish]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package src

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"time"
)

const VERSION = "v2.0.5"
const BIFROST_VERION = "v2.0.5"

func init() {
	pluginDriver.Register("kafka", NewConn, VERSION, BIFROST_VERION)
}

const (
	RUNNING int8 = 1
	CLOSED  int8 = 0
)

type Conn struct {
	pluginDriver.PluginDriverInterface
	Uri      *string
	status   int8
	err      error
	p        *PluginParam
	producer sarama.SyncProducer
}

type PluginParam struct {
	OtherObjectType      pluginDriver.OtherObjectType
	Topic                string
	Key                  string
	BatchSize            int
	Timeout              int
	RequiredAcks         sarama.RequiredAcks
	BifrostFilterQuery   bool // bifrost server 保留,是否过滤sql事件
	BifrostMustBeSuccess bool // bifrost server 保留,数据是否能丢

	dataList         []*sarama.ProducerMessage
	commitBinlogList []*pluginDriver.PluginDataType
}

func NewConn() pluginDriver.Driver {
	f := &Conn{
		status: CLOSED,
	}
	return f
}

func (This *Conn) SetOption(uri *string, param map[string]interface{}) {
	This.Uri = uri
	return
}

func (This *Conn) Open() error {
	This.Connect()
	return nil
}

func (This *Conn) GetUriExample() string {
	return "127.0.0.1:9092,127.0.0.1:9093"
}

func (This *Conn) CheckUri() error {
	config, err := getKafkaConnectConfig(ParseDSN(*This.Uri))
	if err != nil {
		This.err = err
		return err
	}
	config.ConnectConfig.Producer.Return.Successes = true
	config.ConnectConfig.Producer.Return.Errors = true
	producer, err := sarama.NewSyncProducer(config.BrokerServerList, config.ConnectConfig)
	if err == nil {
		producer.Close()
	}
	return err
}

func (This *Conn) newProducer() bool {
	config, err := getKafkaConnectConfig(ParseDSN(*This.Uri))
	if err != nil {
		return false
	}
	config.ConnectConfig.Producer.Return.Successes = true
	config.ConnectConfig.Producer.Return.Errors = true
	config.ConnectConfig.Producer.RequiredAcks = This.p.RequiredAcks
	config.ConnectConfig.Producer.Timeout = time.Duration(This.p.Timeout) * time.Second
	//config.ConnectConfig.Producer.Partitioner = sarama.NewRandomPartitioner
	This.producer, This.err = sarama.NewSyncProducer(config.BrokerServerList, config.ConnectConfig)
	if This.err == nil {
		This.status = RUNNING
		return true
	} else {
		return false
	}
}

func (This *Conn) Connect() bool {
	This.err = fmt.Errorf("no producer")
	This.status = CLOSED
	return true
}

func (This *Conn) GetParam(p interface{}) (interface{}, error) {
	s, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	//var param *PluginParam
	param := &PluginParam{RequiredAcks: -1, Timeout: 10}
	err2 := json.Unmarshal(s, param)
	if err2 != nil {
		return nil, err2
	}
	if param.BatchSize <= 0 {
		param.BatchSize = 1
	}
	if param.Timeout == 0 {
		param.Timeout = 10
	}
	if param.Timeout < 0 {
		param.Timeout = 0
	}
	switch param.RequiredAcks {
	case sarama.NoResponse, sarama.WaitForAll, sarama.WaitForLocal:
		break
	default:
		param.RequiredAcks = sarama.WaitForAll
		break
	}
	if len(param.dataList) == 0 {
		param.dataList = make([]*sarama.ProducerMessage, 0)
		param.commitBinlogList = make([]*pluginDriver.PluginDataType, 0)
	}
	This.p = param
	return param, nil
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

func (This *Conn) ReConnect() bool {
	func() {
		defer func() {
			if err := recover(); err != nil {
				return
			}
		}()
		if This.producer != nil {
			This.producer.Close()
		}
	}()
	r := This.newProducer()
	if r == true {
		return true
	} else {
		return false
	}
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

func (This *Conn) Insert(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return This.sendToList(data, retry, false)
}

func (This *Conn) Update(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return This.sendToList(data, retry, false)
}

func (This *Conn) Del(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return This.sendToList(data, retry, false)
}

func (This *Conn) Query(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return This.sendToList(data, retry, false)
}

func (This *Conn) Commit(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return This.sendToList(data, retry, true)
}

func (This *Conn) getMsg(data *pluginDriver.PluginDataType) (*sarama.ProducerMessage, error) {
	Topic := fmt.Sprint(pluginDriver.TransfeResult(This.p.Topic, data, len(data.Rows)-1))
	msg := &sarama.ProducerMessage{}
	msg.Topic = Topic
	if This.p.Key != "" {
		Key := fmt.Sprint(pluginDriver.TransfeResult(This.p.Key, data, len(data.Rows)-1))
		msg.Key = sarama.StringEncoder(Key)
	}
	toOtherObjectTypeData, _ := pluginDriver.ToOtherObject(data, This.p.OtherObjectType)
	c, err := json.Marshal(toOtherObjectTypeData)
	if err != nil {
		return nil, err
	}
	msg.Value = sarama.StringEncoder(c)
	return msg, nil
}

func (This *Conn) sendToList(data *pluginDriver.PluginDataType, retry bool, isCommit bool) (LastSuccessCommitData *pluginDriver.PluginDataType, Errdata *pluginDriver.PluginDataType, err error) {
	if data == nil && retry == true {
		LastSuccessCommitData, err = This.sendToKafkaByBatch()
		goto endErr
	}
	if This.p.BatchSize > 1 {
		if retry == false {
			var msg *sarama.ProducerMessage
			// 假如 非 commit 事件 或者 没有过滤 sql 事件，则需要将数据放到  list 里
			if !isCommit || !This.p.BifrostFilterQuery {
				msg, err = This.getMsg(data)
				if err != nil {
					goto endErr
				}
				This.p.dataList = append(This.p.dataList, msg)
			}
			if isCommit {
				n0 := len(This.p.dataList) / This.p.BatchSize
				// 计算出 commit 提交是在哪一个 合并组里
				if len(This.p.commitBinlogList)-1 < n0 {
					This.p.commitBinlogList = append(This.p.commitBinlogList, data)
				} else {
					This.p.commitBinlogList[n0] = data
				}
			}
		}
		if len(This.p.dataList) >= This.p.BatchSize {
			LastSuccessCommitData, err = This.sendToKafkaByBatch()
		}
	} else {
		if isCommit && This.p.BifrostFilterQuery {
			return LastSuccessCommitData, nil, nil
		}
		var msg *sarama.ProducerMessage
		msg, err = This.getMsg(data)
		if err != nil {
			goto endErr
		}
		if This.status != RUNNING {
			This.ReConnect()
			if This.status != RUNNING {
				err = This.err
				goto endErr
			}
		}
		_, _, err = This.producer.SendMessage(msg)
		if err == nil {
			LastSuccessCommitData = data
		}
	}
endErr:
	if err != nil {
		if !This.p.BifrostMustBeSuccess {
			return LastSuccessCommitData, nil, nil
		}
		if This.err != nil {
			This.status = CLOSED
			return nil, nil, This.err
		}
		return nil, nil, err
	}
	return LastSuccessCommitData, nil, nil
}

func (This *Conn) sendToKafkaByBatch() (*pluginDriver.PluginDataType, error) {
	if This.status != RUNNING {
		This.ReConnect()
		if This.status != RUNNING {
			return nil, This.err
		}
	}
	if len(This.p.dataList) == 0 {
		return nil, nil
	}
	var err error
	var binlogEvent *pluginDriver.PluginDataType
	if len(This.p.dataList) > This.p.BatchSize {
		list := This.p.dataList[:This.p.BatchSize]
		err = This.producer.SendMessages(list)
		if err == nil {
			This.p.dataList = This.p.dataList[This.p.BatchSize:]
			if len(This.p.commitBinlogList) > 0 {
				binlogEvent = This.p.commitBinlogList[0]
				This.p.commitBinlogList = This.p.commitBinlogList[1:]
			}
		}
	} else {
		err = This.producer.SendMessages(This.p.dataList)
		if err == nil {
			This.p.dataList = make([]*sarama.ProducerMessage, 0)
			if len(This.p.commitBinlogList) > 0 {
				binlogEvent = This.p.commitBinlogList[0]
				This.p.commitBinlogList = This.p.commitBinlogList[1:]
			}
		}
	}
	if err != nil {
		return nil, err
	}
	if binlogEvent != nil {
		return binlogEvent, nil
	} else {
		return nil, nil
	}
}

func (This *Conn) TimeOutCommit() (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return This.sendToList(nil, true, false)
}
