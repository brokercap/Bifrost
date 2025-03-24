package mongo

import (
	"context"
	"errors"
	"fmt"
	"github.com/agiledragon/gomonkey/v2"
	inputDriver "github.com/brokercap/Bifrost/input/driver"
	outputDriver "github.com/brokercap/Bifrost/plugin/driver"
	"github.com/rwynn/gtm/v2"
	. "github.com/smartystreets/goconvey/convey"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"reflect"
	"testing"
	"time"
)

/*
func monitorDump(reslut chan *inputDriver.PluginStatus, plugin inputDriver.Driver, t *testing.T) (r bool) {
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for {
		select {
		case v := <-reslut:
			timer.Reset(3 * time.Second)
			t.Log("status:", v)
		case <-timer.C:
			timer.Reset(3 * time.Second)
			p, _ := plugin.GetCurrentPosition()
			if p == nil {
				continue
			}
			t.Log("position:", *p)
			break
		}
	}
}
*/

/*
func callback(data *outputDriver.PluginDataType) {
	log.Println("callback data:", *data)
}

func TestMongoInput_Start(t *testing.T) {
	inputInfo := inputDriver.InputInfo{
		ConnectUri:     "mongodb://192.168.137.130:27017",
		GTID:           "",
		BinlogFileName: "mysql-bin.000001",
		BinlogPostion:  0,
		ServerId:       366,
	}
	ch := make(chan *inputDriver.PluginStatus, 2)
	plugin := NewInputPlugin()
	plugin.SetEventID(0)
	plugin.SetOption(inputInfo, nil)
	plugin.SetCallback(callback)
	go plugin.Start(ch)
	go monitorDump(ch, plugin, t)
	time.Sleep(1000 * time.Second)
}
*/

func TestMongoInput_GetUriExample(t *testing.T) {
	c := new(MongoInput)
	Convey("normal", t, func() {
		uri, html := c.GetUriExample()
		So(uri, ShouldNotEqual, "")
		So(html, ShouldNotEqual, "")
	})
}

func TestMongoInput_SetOption(t *testing.T) {
	c := new(MongoInput)
	patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "GTID2OpLogPosition", func(c *MongoInput, GTID string) *primitive.Timestamp {
		return nil
	})
	defer patches.Reset()
	Convey("normal", t, func() {
		c.SetOption(inputDriver.InputInfo{}, nil)
	})
}

func TestMongoInput_setStatus(t *testing.T) {
	c := new(MongoInput)
	Convey("status close", t, func() {
		c.setStatus(inputDriver.CLOSED)
		So(c.err, ShouldNotBeNil)
	})
	Convey("PluginStatusChan is nil", t, func() {
		c.setStatus(inputDriver.STARTING)
	})
	Convey("PluginStatusChan is not nil", t, func() {
		ch := make(chan *inputDriver.PluginStatus, 2)
		c.PluginStatusChan = ch
		c.setStatus(inputDriver.STARTING)
		var hadVal = false
		select {
		case <-ch:
			hadVal = true
			break
		default:
			break
		}
		So(hadVal, ShouldEqual, true)
	})
}

func TestMongoInput_Start(t *testing.T) {
	Convey("BatchAndReplicate", t, func() {
		c := new(MongoInput)
		c.inputInfo.GTID = BatchAndReplicate
		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "StartBatchAndReplicate", func(c *MongoInput) error {
			return errors.New(BatchAndReplicate)
		})
		defer patches.Reset()
		err := c.Start(make(chan *inputDriver.PluginStatus, 2))
		So(err.Error(), ShouldEqual, BatchAndReplicate)
	})

	Convey("OnlyBatch", t, func() {
		c := new(MongoInput)
		c.inputInfo.GTID = OnlyBatch
		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "StartOnlyBatch", func(c *MongoInput) error {
			return errors.New(OnlyBatch)
		})
		defer patches.Reset()
		err := c.Start(make(chan *inputDriver.PluginStatus, 2))
		So(err.Error(), ShouldEqual, OnlyBatch)
	})

	Convey("OnlyReplicate", t, func() {
		c := new(MongoInput)
		c.inputInfo.GTID = ""
		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "StartOnlyReplicate", func(c *MongoInput) error {
			return errors.New("OnlyReplicate")
		})
		defer patches.Reset()
		err := c.Start(make(chan *inputDriver.PluginStatus, 2))
		So(err.Error(), ShouldEqual, "OnlyReplicate")
	})
}

/*
func TestMongoInput_Start_with_panic(t *testing.T) {
	Convey("panic", t, func() {
		c := new(MongoInput)
		c.PluginStatusChan = make(chan *inputDriver.PluginStatus, 10)
		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "StartOnlyReplicate", func(c *MongoInput) error {
			panic("panic test")
		})
		defer patches.Reset()
		err := c.Start(make(chan *inputDriver.PluginStatus, 2))
		select {
		case status := <-c.PluginStatusChan:
			So(status.Status, ShouldEqual, inputDriver.CLOSED)
			So(err, ShouldBeNil)
			break
		case <-time.After(10 * time.Second):
			t.Fatal("test time out")
		}
	})
}
*/

func TestMongoInput_StartBatchAndReplicate(t *testing.T) {
	Convey("get current position error", t, func() {
		c := new(MongoInput)
		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "GetCurrentPosition", func(c *MongoInput) (p *inputDriver.PluginPosition, err error) {
			return nil, errors.New("error")
		})
		defer patches.Reset()
		err := c.StartBatchAndReplicate()
		So(err, ShouldNotBeNil)
	})

	Convey("batch error", t, func() {
		c := new(MongoInput)
		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "GetCurrentPosition", func(c *MongoInput) (p *inputDriver.PluginPosition, err error) {
			p = &inputDriver.PluginPosition{
				GTID: "{\"T\":1696329531,\"I\":0}",
			}
			return
		})
		patches.ApplyMethod(reflect.TypeOf(c), "BatchStart", func(c *MongoInput) (err error) {
			return errors.New("error")
		})
		defer patches.Reset()
		err := c.StartBatchAndReplicate()
		So(err, ShouldNotBeNil)
	})

	Convey("StartOnlyReplicate error", t, func() {
		c := new(MongoInput)
		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "GetCurrentPosition", func(c *MongoInput) (p *inputDriver.PluginPosition, err error) {
			p = &inputDriver.PluginPosition{
				GTID: "{\"T\":1696329531,\"I\":0}",
			}
			return
		})
		patches.ApplyMethod(reflect.TypeOf(c), "BatchStart", func(c *MongoInput) (err error) {
			return nil
		})
		patches.ApplyMethod(reflect.TypeOf(c), "StartOnlyReplicate", func(c *MongoInput) (err error) {
			return errors.New("error")
		})
		defer patches.Reset()
		err := c.StartBatchAndReplicate()
		So(err, ShouldNotBeNil)
		So(c.GetLastPosition(), ShouldNotBeNil)
		So(c.GetLastPosition().Timestamp, ShouldEqual, 1696329531)
	})
}

func TestMongoInput_StartOnlyBatch(t *testing.T) {
	Convey("error", t, func() {
		c := new(MongoInput)
		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "BatchStart", func(c *MongoInput) error {
			return errors.New("error")
		})
		defer patches.Reset()
		err := c.StartOnlyBatch()
		So(err, ShouldNotBeNil)
	})

	Convey("nil", t, func() {
		c := new(MongoInput)
		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "BatchStart", func(c *MongoInput) error {
			return nil
		})
		defer patches.Reset()
		err := c.StartOnlyBatch()
		So(err, ShouldBeNil)
	})
}

func TestMongoInput_StartOnlyReplicate(t *testing.T) {
	c := new(MongoInput)
	patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "StartOnlyReplicate0", func(c *MongoInput) error {
		return nil
	})
	defer patches.Reset()
	Convey("time out", t, func() {
		ctx, _ := context.WithTimeout(context.Background(), 4*time.Second)
		go func() {
			_ = c.StartOnlyReplicate()
			ctx.Done()
		}()
		select {
		case _ = <-ctx.Done():
			t.Log("success")
		case _ = <-time.After(8 * time.Second):
			t.Error("c.StartOnlyReplicate time out")
		}
	})

	Convey("by stop", t, func() {
		go func() {
			_ = c.StartOnlyReplicate()
		}()
		// 这里睡眠1秒，是为了防止协程里修改了c.ctx值，但是在主线程中没被更新，而导致异常
		time.Sleep(1 * time.Second)
		c.ctxCancleFun()
		select {
		case _ = <-c.ctx.Done():
			t.Log("success")
		case _ = <-time.After(8 * time.Second):
			t.Error("c.StartOnlyReplicate time out")
		}
	})

}

func TestMongoInput_StartOnlyReplicate0(t *testing.T) {
	c := new(MongoInput)

	Convey("CreateMongoClient error", t, func() {
		patches := gomonkey.ApplyFunc(CreateMongoClient, func(uri string, ctx context.Context) (*mongo.Client, error) {
			return &mongo.Client{}, fmt.Errorf("mock error")
		})
		defer patches.Reset()
		err := c.StartOnlyReplicate0()
		So(err, ShouldNotBeNil)
	})

	Convey("normal", t, func() {
		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "ConsumeMongoOpLog", func(c *MongoInput, ctx *gtm.OpCtx) {
			return
		})
		patches.ApplyMethod(reflect.TypeOf(c), "GtmAfter", func(c *MongoInput, client *mongo.Client, options *gtm.Options) (primitive.Timestamp, error) {
			return primitive.Timestamp{}, nil
		})
		patches.ApplyFunc(CreateMongoClient, func(uri string, ctx context.Context) (*mongo.Client, error) {
			return &mongo.Client{}, nil
		})
		defer patches.Reset()
		err := c.StartOnlyReplicate0()
		So(err, ShouldBeNil)
	})
}

func TestMongoInput_GtmAfter(t *testing.T) {
	c := new(MongoInput)

	Convey("return", t, func() {
		c.currentPosition = &primitive.Timestamp{}
		_, err := c.GtmAfter(&mongo.Client{}, &gtm.Options{})
		So(err, ShouldBeNil)
	})

}

func TestMongoInput_OpFitler(t *testing.T) {

	Convey("Operation c false", t, func() {
		c := new(MongoInput)
		op := &gtm.Op{}
		op.Operation = "c"

		patches := gomonkey.ApplyMethod(reflect.TypeOf(op), "IsDropCollection", func(op *gtm.Op) (string, bool) {
			return "", false
		})
		patches.ApplyMethod(reflect.TypeOf(op), "GetDatabase", func(op *gtm.Op) string {
			return "database"
		})
		patches.ApplyMethod(reflect.TypeOf(op), "IsDropDatabase", func(op *gtm.Op) (string, bool) {
			return "database", true
		})
		b := c.OpFitler(op)
		So(b, ShouldEqual, true)
	})

	Convey("Operation insert true", t, func() {
		c := new(MongoInput)
		op := &gtm.Op{}
		op.Operation = "i"

		patches := gomonkey.ApplyMethod(reflect.TypeOf(op), "GetCollection", func(op *gtm.Op) string {
			return "table"
		})
		patches.ApplyMethod(reflect.TypeOf(op), "GetDatabase", func(op *gtm.Op) string {
			return "database"
		})
		b := c.OpFitler(op)
		So(b, ShouldEqual, true)
	})

}

func TestMongoInput_ConsumeMongoOpLog(t *testing.T) {

	Convey("OpCtx done", t, func() {
		c := new(MongoInput)
		c.PluginStatusChan = make(chan *inputDriver.PluginStatus, 10)
		c.ctx, c.ctxCancleFun = context.WithTimeout(context.Background(), 20*time.Second)
		opCtx := &gtm.OpCtx{ErrC: make(chan error, 100)}
		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "ToInputCallback", func(c *MongoInput, op *gtm.Op) {
			return
		})
		defer patches.Reset()
		opCtx.ErrC <- fmt.Errorf("mock error")
		close(opCtx.ErrC)
		c.ConsumeMongoOpLog(opCtx)
	})

	Convey("OpC msg", t, func() {
		c := new(MongoInput)
		c.PluginStatusChan = make(chan *inputDriver.PluginStatus, 10)
		c.ctx, c.ctxCancleFun = context.WithTimeout(context.Background(), 20*time.Second)
		opCtx := &gtm.OpCtx{ErrC: make(chan error, 100)}
		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "ToInputCallback", func(c *MongoInput, op *gtm.Op) {
			return
		})
		defer patches.Reset()
		go func() {
			opCtx.OpC <- &gtm.Op{}
			c.ctxCancleFun()
		}()
		c.ConsumeMongoOpLog(opCtx)
	})
}

func TestMongoInput_Stop(t *testing.T) {
	c := new(MongoInput)
	Convey("return", t, func() {
		c.PluginStatusChan = make(chan *inputDriver.PluginStatus, 10)
		c.ctx, c.ctxCancleFun = context.WithTimeout(context.Background(), 20*time.Second)
		So(c.Stop(), ShouldBeNil)
	})
}

func TestMongoInput_Close(t *testing.T) {
	c := new(MongoInput)
	Convey("return", t, func() {
		c.PluginStatusChan = make(chan *inputDriver.PluginStatus, 10)
		c.ctx, c.ctxCancleFun = context.WithTimeout(context.Background(), 20*time.Second)
		So(c.Close(), ShouldBeNil)
	})
}

func TestMongoInput_Kill(t *testing.T) {
	c := new(MongoInput)
	Convey("return", t, func() {
		c.PluginStatusChan = make(chan *inputDriver.PluginStatus, 10)
		c.ctx, c.ctxCancleFun = context.WithTimeout(context.Background(), 20*time.Second)
		So(c.Kill(), ShouldBeNil)
	})
}

func TestMongoInput_GetLastPosition(t *testing.T) {
	c := &MongoInput{}
	Convey("lastOp nil", t, func() {
		So(c.GetLastPosition(), ShouldBeNil)
	})

	Convey("lastOp is not nil", t, func() {
		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "OpLogPosition2GTID", func(c *MongoInput, p *primitive.Timestamp) string {
			return ""
		})
		defer patches.Reset()
		c.lastOp = &gtm.Op{}
		So(c.GetLastPosition(), ShouldNotBeNil)
	})
}

func TestMongoInput_SetCallback(t *testing.T) {
	c := &MongoInput{}
	Convey("callback nil", t, func() {
		c.SetCallback(nil)
	})
	Convey("callback is function", t, func() {
		var callback = func(data *outputDriver.PluginDataType) {
			return
		}
		c.SetCallback(callback)
	})
}

func TestMongoInput_SetEventID(t *testing.T) {
	c := &MongoInput{}
	Convey("set 0", t, func() {
		c.SetEventID(0)
	})
}

func TestMongoInput_getNextEventID(t *testing.T) {
	c := &MongoInput{}
	Convey("set 0", t, func() {
		c.SetEventID(0)
		id := c.getNextEventID()
		So(id, ShouldEqual, 1)
	})
}
