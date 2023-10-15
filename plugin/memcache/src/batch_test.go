package src

import (
	"errors"
	"fmt"
	"github.com/agiledragon/gomonkey"
	"github.com/brokercap/Bifrost/plugin/driver"
	"github.com/brokercap/Bifrost/sdk/pluginTestData"
	. "github.com/smartystreets/goconvey/convey"
	"reflect"
	"testing"
)

func TestConn_CommitBatch(t *testing.T) {
	e := pluginTestData.NewEvent()
	dataList := make([]*driver.PluginDataType, 0)
	dataList = append(dataList, e.GetTestInsertData())
	dataList = append(dataList, e.GetTestInsertData())
	dataList = append(dataList, e.GetTestUpdateData())
	dataList = append(dataList, e.GetTestUpdateData())
	dataList = append(dataList, e.GetTestCommitData())
	dataList = append(dataList, e.GetTestQueryData())
	dataList = append(dataList, e.GetTestDeleteData())
	dataList = append(dataList, e.GetTestCommitData())

	keysConfig := ""
	for _, key := range dataList[0].Pri {
		keysConfig += fmt.Sprintf("-{$%s}", key)
	}

	Convey("set normal", t, func() {
		c := &Conn{
			p: &PluginParam{
				KeyConfig: keysConfig,
			},
		}

		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "Update", func(c *Conn, data *driver.PluginDataType, retry bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
			return nil, nil, nil
		})
		patches.ApplyMethod(reflect.TypeOf(c), "Del", func(c *Conn, data *driver.PluginDataType, retry bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
			return nil, nil, nil
		})
		patches.ApplyMethod(reflect.TypeOf(c), "Query", func(c *Conn, data *driver.PluginDataType, retry bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
			return nil, nil, nil
		})
		patches.ApplyMethod(reflect.TypeOf(c), "Commit", func(c *Conn, data *driver.PluginDataType, retry bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
			return data, nil, nil
		})
		defer patches.Reset()

		LastSuccessCommitData, ErrData, Err := c.CommitBatch(dataList, false)
		So(Err, ShouldBeNil)
		So(ErrData, ShouldBeNil)
		So(LastSuccessCommitData, ShouldNotBeNil)
		So(LastSuccessCommitData.BinlogPosition, ShouldEqual, dataList[len(dataList)-1].BinlogPosition)
	})

	Convey("errors", t, func() {
		c := &Conn{
			p: &PluginParam{
				KeyConfig: keysConfig,
			},
		}

		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "Update", func(c *Conn, data *driver.PluginDataType, retry bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
			return nil, data, errors.New("update error")
		})
		patches.ApplyMethod(reflect.TypeOf(c), "Del", func(c *Conn, data *driver.PluginDataType, retry bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
			return nil, nil, nil
		})
		patches.ApplyMethod(reflect.TypeOf(c), "Query", func(c *Conn, data *driver.PluginDataType, retry bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
			return nil, nil, nil
		})
		patches.ApplyMethod(reflect.TypeOf(c), "Commit", func(c *Conn, data *driver.PluginDataType, retry bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
			return data, nil, nil
		})
		defer patches.Reset()

		LastSuccessCommitData, ErrData, Err := c.CommitBatch(dataList, false)
		So(Err, ShouldNotBeNil)
		So(ErrData, ShouldNotBeNil)
		So(LastSuccessCommitData, ShouldBeNil)
	})
}
