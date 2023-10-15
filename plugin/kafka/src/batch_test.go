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
	dataList = append(dataList, e.GetTestUpdateData())
	dataList = append(dataList, e.GetTestDeleteData())
	dataList = append(dataList, e.GetTestCommitData())
	dataList = append(dataList, e.GetTestQueryData())
	dataList = append(dataList, e.GetTestCommitData())

	convey.Convey("normal insert,update,delete,commit,query,commit", t, func() {
		c := &Conn{
			p: &PluginParam{},
		}
		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "SendToList", func(c *Conn, data *driver.PluginDataType, retry bool, isCommit bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
			if isCommit {
				return data, nil, nil
			}
			return nil, nil, nil
		})
		defer patches.Reset()
		LastSuccessCommitData, ErrData, err := c.CommitBatch(dataList, false)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ErrData, convey.ShouldBeNil)
		convey.So(LastSuccessCommitData, convey.ShouldNotBeNil)
		convey.So(LastSuccessCommitData.BinlogPosition, convey.ShouldEqual, dataList[5].BinlogPosition)
	})

	convey.Convey("error", t, func() {
		c := &Conn{
			p: &PluginParam{},
		}
		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "SendToList", func(c *Conn, data *driver.PluginDataType, retry bool, isCommit bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
			return nil, data, errors.New("error")
		})
		defer patches.Reset()
		LastSuccessCommitData, ErrData, err := c.CommitBatch(dataList, false)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(ErrData, convey.ShouldNotBeNil)
		convey.So(ErrData.BinlogPosition, convey.ShouldEqual, dataList[0].BinlogPosition)
		convey.So(LastSuccessCommitData, convey.ShouldBeNil)
	})
}
