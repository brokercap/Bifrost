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

package mongo

import (
	"context"
	"fmt"
	inputDriver "github.com/brokercap/Bifrost/input/driver"
	"github.com/rwynn/gtm/v2"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"sync"
	"sync/atomic"
	"time"
)

func NewInputPlugin() inputDriver.Driver {
	return &MongoInput{}
}

type MongoInput struct {
	sync.RWMutex
	inputDriver.PluginDriverInterface
	inputInfo       inputDriver.InputInfo
	currentPosition *primitive.Timestamp

	status           inputDriver.StatusFlag
	err              error
	PluginStatusChan chan *inputDriver.PluginStatus
	eventID          uint64
	callback         inputDriver.Callback

	ctx          context.Context
	ctxCancleFun context.CancelFunc

	lastOp *gtm.Op
}

func (c *MongoInput) GetUriExample() (string, string) {
	notesHtml := `<p>mongo源当前不会校验是否开启了Oplog等，请自行确保开启了Oplog再使用</p>
	<p>mongo源delete事件，并不会返回_id字段之外的其他数据，使用的时候请注意</p>
	<p>请使用upsert进行修改数据,则可以正常同步所有字段,例如：</p>
	<p style="color:#F00;font-weight:bold">db.bifrost_field_test.update({"name":"bifrost"},{$set:{version:"v2.x"}});</p>
    <p>如果使用以下方式更新数据，则获取不到旧数据</p>
    <p>db.bifrost_field_test.update({"name":"bifrost"},{version:"v2.x"});</p>
	`
	exampleUri := "mongodb://[user:pass@]host1[:port1][,host2[:port2],...][/database][?options]"
	return exampleUri, notesHtml
}

func (c *MongoInput) Init() {

}

func (c *MongoInput) SetOption(inputInfo inputDriver.InputInfo, param map[string]interface{}) {
	c.inputInfo = inputInfo
	c.currentPosition = c.GTID2OpLogPosition(inputInfo.GTID)
}

func (c *MongoInput) setStatus(status inputDriver.StatusFlag) {
	c.status = status
	switch status {
	case inputDriver.CLOSED:
		c.err = fmt.Errorf("")
		break
	}
	if c.PluginStatusChan != nil {
		c.PluginStatusChan <- &inputDriver.PluginStatus{Status: status, Error: c.err}
	}
}

func (c *MongoInput) Start(ch chan *inputDriver.PluginStatus) error {
	c.PluginStatusChan = ch
	return c.Start0()
}

func (c *MongoInput) Start0() error {
	c.ctx, c.ctxCancleFun = context.WithCancel(context.Background())
	var timeout = 2 * time.Second
	var timer = time.NewTimer(timeout)
	for {
		c.setStatus(inputDriver.STARTING)
		c.Start1()
		timer.Reset(timeout)
		select {
		case <-c.ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
			timer.Stop()
			break
		}
	}
	return nil
}

func (c *MongoInput) Start1() error {
	client, err := CreateMongoClient(c.inputInfo.ConnectUri, c.ctx)
	if err != nil {
		return err
	}
	defer client.Disconnect(c.ctx)
	var after gtm.TimestampGenerator
	// after == nil, 则默认采用最新的位点
	// after 函数返回空位点，则是采用oplog中最早的位点，nil 和 空字符串是有区别的
	if c.currentPosition != nil {
		after = c.GtmAfter
	}
	ctx := gtm.Start(
		client,
		&gtm.Options{
			After:             after,
			Filter:            c.OpFitler,
			MaxAwaitTime:      3 * time.Second,
			OpLogDisabled:     false,
			UpdateDataAsDelta: false,
		},
	)
	c.setStatus(inputDriver.RUNNING)
	c.ConsumeMongoOpLog(ctx)
	return nil
}

func (c *MongoInput) GtmAfter(client *mongo.Client, options *gtm.Options) (primitive.Timestamp, error) {
	return *c.currentPosition, nil
}

func (c *MongoInput) OpFitler(op *gtm.Op) bool {
	var schemaName = op.GetDatabase()
	var table string
	switch op.Operation {
	case "c":
		// &{Id: Operation:c Namespace:test.$cmd Data:map[drop:mytb] Timestamp:{T:1679727658 I:1} Source:0 Doc:map[drop:mytb] UpdateDescription:map[] ResumeToken:{StreamID: ResumeToken:<nil>}}
		// drop table 事件　Namespace 里是没有保存表名的
		var ok bool
		table, ok = op.IsDropCollection()
		if !ok {
			var dropDatabseName string
			dropDatabseName, ok = op.IsDropDatabase()
			if ok {
				schemaName = dropDatabseName
			}
		}
	default:
		table = op.GetCollection()
	}
	//log.Println(schemaName, "table:", table)
	if c.CheckReplicateDb(schemaName, table) {
		return true
	}
	// 假如不需要同步的库表，也需要更新保存最后一条op数据，反之则是在被处理处理之后再保存
	// lastOpLog 是用于记录最后处理数据的位点使用
	c.setLastOpLog(op)
	return false
}

func (c *MongoInput) ConsumeMongoOpLog(ctx *gtm.OpCtx) {
	for {
		select {
		case c.err = <-ctx.ErrC:
			if c.err == nil {
				return
			}
			c.PluginStatusChan <- &inputDriver.PluginStatus{
				Status: c.status,
				Error:  c.err,
			}
			break
		case <-c.ctx.Done():
			return
		case op := <-ctx.OpC:
			c.ToInputCallback(op)
			c.setLastOpLog(op)
			break
		}

	}
}

func (c *MongoInput) setLastOpLog(op *gtm.Op) {
	c.Lock()
	c.lastOp = op
	c.Unlock()
	return
}

func (c *MongoInput) Stop() error {
	c.setStatus(inputDriver.STOPPING)
	if c.ctxCancleFun != nil {
		c.ctxCancleFun()
	}
	c.ctxCancleFun = nil
	c.setStatus(inputDriver.STOPPED)
	return nil
}

func (c *MongoInput) Close() error {
	c.setStatus(inputDriver.CLOSED)
	return nil
}

func (c *MongoInput) Kill() error {
	c.Stop()
	c.Close()
	return nil
}

func (c *MongoInput) GetLastPosition() *inputDriver.PluginPosition {
	c.RLock()
	defer c.RUnlock()
	if c.lastOp == nil {
		return nil
	}
	return &inputDriver.PluginPosition{
		GTID:           c.OpLogPosition2GTID(&c.lastOp.Timestamp),
		BinlogFileName: c.inputInfo.BinlogFileName,
		BinlogPostion:  c.inputInfo.BinlogPostion,
		Timestamp:      c.lastOp.Timestamp.T,
		EventID:        c.eventID,
	}
}

func (c *MongoInput) SetCallback(callback inputDriver.Callback) {
	c.callback = callback
}

func (c *MongoInput) SetEventID(eventId uint64) error {
	c.eventID = eventId
	return nil
}

func (c *MongoInput) getNextEventID() uint64 {
	return atomic.AddUint64(&c.eventID, 1)
}
