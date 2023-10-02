package src

import (
	"github.com/brokercap/Bifrost/sdk/pluginTestData"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

var starrockUri = "root:@tcp(127.0.0.1:9030)/starrocks"

func TestConn_StarrocksNotAutoTableCommit_Integration(t *testing.T) {

	Convey("normal", t, func() {
		myConn := NewConn()
		myConn.SetOption(&starrockUri, nil)
		myConn.Open()

		var SchemaName = "mytest"
		var TableName = "tb_1"

		e := pluginTestData.NewEvent()
		e.SetSchema(SchemaName)
		e.SetTable(TableName)

		param := map[string]interface{}{
			"Field":               make([]map[string]string, 0),
			"Schema":              "",
			"Table":               "",
			"BatchSize":           1000,
			"SyncMode":            "Normal",
			"NullTransferDefault": false,
		}
		_, err := myConn.SetParam(param)
		So(err, ShouldBeNil)

		myConn.Insert(e.GetTestInsertData(), false)
		myConn.Insert(e.GetTestInsertData(), false)
		_, _, err2 := myConn.TimeOutCommit()
		So(err2, ShouldBeNil)
		myConn.Insert(e.GetTestDeleteData(), false)
		_, _, err2 = myConn.TimeOutCommit()
		So(err2, ShouldBeNil)
	})
	/*
		Convey("normal_update", t, func() {
			myConn := NewConn()
			myConn.SetOption(&starrockUri, nil)
			myConn.Open()

			var SchemaName = "mytest"
			var TableName = "tb_normal_update"

			e := pluginTestData.NewEvent()
			e.SetSchema(SchemaName)
			e.SetTable(TableName)

			param := map[string]interface{}{
				"Field":               make([]map[string]string, 0),
				"Schema":              "",
				"Table":               "",
				"BatchSize":           1000,
				"SyncMode":            "LogUpdate",
				"NullTransferDefault": false,
			}
			_, err := myConn.SetParam(param)
			So(err, ShouldBeNil)

			myConn.Insert(e.GetTestInsertData(), false)
			_, _, err2 := myConn.TimeOutCommit()
			So(err2, ShouldBeNil)
			myConn.Insert(e.GetTestDeleteData(), false)
			_, _, err2 = myConn.TimeOutCommit()
			So(err2, ShouldBeNil)
		})
	*/
}

func TestConn_StarrocksNotAutoTableCommit_With_Append_Integration(t *testing.T) {
	Convey("append", t, func() {
		myConn := NewConn()
		myConn.SetOption(&starrockUri, nil)
		myConn.Open()

		var SchemaName = "mytest"
		var TableName = "append_tb_1"
		var TableName2 = "append_tb_2"

		e := pluginTestData.NewEvent()
		e.SetSchema(SchemaName)
		e.SetTable(TableName)

		e2 := pluginTestData.NewEvent()
		e2.SetSchema(SchemaName)
		e2.SetTable(TableName2)

		param := map[string]interface{}{
			"Field":               make([]map[string]string, 0),
			"Schema":              "",
			"Table":               "",
			"BatchSize":           1000,
			"SyncMode":            "LogAppend",
			"NullTransferDefault": false,
		}
		_, err := myConn.SetParam(param)
		So(err, ShouldBeNil)

		myConn.Insert(e.GetTestInsertData(), false)
		myConn.Insert(e2.GetTestInsertData(), false)

		_, _, err2 := myConn.TimeOutCommit()
		So(err2, ShouldBeNil)
	})
}
