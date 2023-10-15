package src

import (
	"errors"
	"github.com/agiledragon/gomonkey"
	"github.com/brokercap/Bifrost/plugin/driver"
	"github.com/brokercap/Bifrost/sdk/pluginTestData"
	"github.com/smartystreets/goconvey/convey"
	"reflect"
	"testing"
)

func TestConn_CommitBatch(t *testing.T) {
	e := pluginTestData.NewEvent()
	dataList := make([]*driver.PluginDataType, 0)
	dataList = append(dataList, e.GetTestInsertData())
	dataList = append(dataList, e.GetTestInsertData())
	dataList = append(dataList, e.GetTestUpdateData())
	dataList = append(dataList, e.GetTestDeleteData())
	dataList = append(dataList, e.GetTestCommitData())
	// 第一次提交
	dataList = append(dataList, e.GetTestQueryData())
	dataList = append(dataList, e.GetTestCommitData())
	dataList = append(dataList, e.GetTestInsertData())
	dataList = append(dataList, e.GetTestCommitData())
	// 第二次提交
	dataList = append(dataList, e.GetTestQueryData())
	// 第三次提交
	dataList = append(dataList, e.GetTestCommitData())

	convey.Convey("only one insert", t, func() {
		dataList0 := dataList[:1]
		c := &Conn{
			p: &PluginParam{},
		}
		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "AutoCommit", func(c *Conn) (*driver.PluginDataType, *driver.PluginDataType, error) {
			return nil, nil, nil
		})
		defer patches.Reset()
		LastSuccessCommitData, ErrData, err := c.CommitBatch(dataList0, false)
		convey.So(err, convey.ShouldBeNil)
		convey.So(LastSuccessCommitData, convey.ShouldBeNil)
		convey.So(ErrData, convey.ShouldBeNil)
	})

	convey.Convey("only one commit", t, func() {
		dataList0 := dataList[:5]
		c := &Conn{
			p: &PluginParam{},
		}
		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "AutoCommit", func(c *Conn) (*driver.PluginDataType, *driver.PluginDataType, error) {
			return nil, nil, nil
		})
		defer patches.Reset()
		LastSuccessCommitData, ErrData, err := c.CommitBatch(dataList0, false)
		convey.So(err, convey.ShouldBeNil)
		convey.So(LastSuccessCommitData, convey.ShouldBeNil)
		convey.So(ErrData, convey.ShouldBeNil)
	})

	convey.Convey("normal insert,update,delete,commit,query,commit", t, func() {
		c := &Conn{
			p: &PluginParam{},
		}
		var AutoCommitI = 0
		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "AutoCommit", func(c *Conn) (*driver.PluginDataType, *driver.PluginDataType, error) {
			AutoCommitI++
			switch AutoCommitI {
			case 1:
				return dataList[4], nil, nil
			case 2:
				return dataList[8], nil, nil
			case 3:
				return dataList[10], nil, nil
			}
			return nil, nil, nil
		})
		var QueryI = 0
		patches.ApplyMethod(reflect.TypeOf(c), "Query", func(c *Conn, data *driver.PluginDataType, retry bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
			QueryI++
			return data, nil, nil
		})
		defer patches.Reset()
		LastSuccessCommitData, ErrData, err := c.CommitBatch(dataList, false)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ErrData, convey.ShouldBeNil)
		convey.So(LastSuccessCommitData, convey.ShouldNotBeNil)
		convey.So(LastSuccessCommitData.BinlogPosition, convey.ShouldEqual, dataList[10].BinlogPosition)
		convey.So(AutoCommitI, convey.ShouldEqual, 3)
		convey.So(QueryI, convey.ShouldEqual, 2)
	})

	convey.Convey("normal query error", t, func() {
		c := &Conn{
			p: &PluginParam{},
		}
		var AutoCommitI = 0
		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "AutoCommit", func(c *Conn) (*driver.PluginDataType, *driver.PluginDataType, error) {
			AutoCommitI++
			switch AutoCommitI {
			case 1:
				return dataList[4], nil, nil
			case 2:
				return dataList[8], nil, nil
			}
			return nil, nil, nil
		})
		var QueryI = 0
		patches.ApplyMethod(reflect.TypeOf(c), "Query", func(c *Conn, data *driver.PluginDataType, retry bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
			QueryI++
			switch QueryI {
			case 1:
				return nil, data, errors.New("query error")
			}
			return nil, nil, nil
		})
		defer patches.Reset()
		LastSuccessCommitData, ErrData, err := c.CommitBatch(dataList, false)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(ErrData, convey.ShouldNotBeNil)
		convey.So(ErrData.BinlogPosition, convey.ShouldEqual, dataList[5].BinlogPosition)
		convey.So(LastSuccessCommitData, convey.ShouldNotBeNil)
		convey.So(LastSuccessCommitData.BinlogPosition, convey.ShouldEqual, dataList[4].BinlogPosition)
		convey.So(AutoCommitI, convey.ShouldEqual, 1)
		convey.So(QueryI, convey.ShouldEqual, 1)
	})

	convey.Convey("second  AutoCommit error", t, func() {
		c := &Conn{
			p: &PluginParam{},
		}
		var AutoCommitI = 0
		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "AutoCommit", func(c *Conn) (*driver.PluginDataType, *driver.PluginDataType, error) {
			AutoCommitI++
			switch AutoCommitI {
			case 1:
				return dataList[4], nil, nil
			case 2:
				return nil, dataList[8], errors.New("AutoCommit error")
			}
			return nil, nil, nil
		})
		var QueryI = 0
		patches.ApplyMethod(reflect.TypeOf(c), "Query", func(c *Conn, data *driver.PluginDataType, retry bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
			QueryI++
			return nil, nil, nil
		})
		defer patches.Reset()
		LastSuccessCommitData, ErrData, err := c.CommitBatch(dataList, false)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(ErrData, convey.ShouldNotBeNil)
		convey.So(ErrData.BinlogPosition, convey.ShouldEqual, dataList[8].BinlogPosition)
		convey.So(LastSuccessCommitData, convey.ShouldNotBeNil)
		convey.So(LastSuccessCommitData.BinlogPosition, convey.ShouldEqual, dataList[4].BinlogPosition)
		convey.So(AutoCommitI, convey.ShouldEqual, 2)
		convey.So(QueryI, convey.ShouldEqual, 1)
	})

	convey.Convey("three  AutoCommit error", t, func() {
		c := &Conn{
			p: &PluginParam{},
		}
		var AutoCommitI = 0
		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "AutoCommit", func(c *Conn) (*driver.PluginDataType, *driver.PluginDataType, error) {
			AutoCommitI++
			switch AutoCommitI {
			case 1:
				return dataList[4], nil, nil
			case 2:
				return dataList[8], nil, nil
			case 3:
				return nil, dataList[10], errors.New("AutoCommit error")
			}
			return nil, nil, nil
		})
		var QueryI = 0
		patches.ApplyMethod(reflect.TypeOf(c), "Query", func(c *Conn, data *driver.PluginDataType, retry bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
			QueryI++
			return nil, nil, nil
		})
		defer patches.Reset()
		LastSuccessCommitData, ErrData, err := c.CommitBatch(dataList, false)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(ErrData, convey.ShouldNotBeNil)
		convey.So(ErrData.BinlogPosition, convey.ShouldEqual, dataList[10].BinlogPosition)
		convey.So(LastSuccessCommitData, convey.ShouldNotBeNil)
		convey.So(LastSuccessCommitData.BinlogPosition, convey.ShouldEqual, dataList[8].BinlogPosition)
		convey.So(AutoCommitI, convey.ShouldEqual, 3)
		convey.So(QueryI, convey.ShouldEqual, 2)
	})
}
