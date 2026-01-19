package src

import (
	"encoding/json"
	"testing"

	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	. "github.com/smartystreets/goconvey/convey"
)

func TestConvertValue(t *testing.T) {
	c := &Conn{}

	Convey("Test convertValue", t, func() {
		Convey("string", func() {
			val, err := c.convertValue("test")
			So(err, ShouldBeNil)
			So(val, ShouldEqual, "test")
		})

		Convey("int", func() {
			val, err := c.convertValue(123)
			So(err, ShouldBeNil)
			So(val, ShouldEqual, 123)
		})

		Convey("float", func() {
			val, err := c.convertValue(123.456)
			So(err, ShouldBeNil)
			So(val, ShouldEqual, 123.456)
		})

		Convey("bool", func() {
			val, err := c.convertValue(true)
			So(err, ShouldBeNil)
			So(val, ShouldEqual, true)
		})

		Convey("[]byte", func() {
			val, err := c.convertValue([]byte("test"))
			So(err, ShouldBeNil)
			So(val, ShouldEqual, "test")
		})

		Convey("json.Number", func() {
			val, err := c.convertValue(json.Number("123.456"))
			So(err, ShouldBeNil)
			So(val, ShouldEqual, "123.456")
		})

		Convey("map", func() {
			m := map[string]interface{}{"key": "value"}
			val, err := c.convertValue(m)
			So(err, ShouldBeNil)
			var resMap map[string]interface{}
			json.Unmarshal([]byte(val.(string)), &resMap)
			So(resMap["key"], ShouldEqual, "value")
		})

		Convey("slice", func() {
			s := []int{1, 2, 3}
			val, err := c.convertValue(s)
			So(err, ShouldBeNil)
			So(val, ShouldEqual, "[1,2,3]")
		})

		Convey("nil", func() {
			val, err := c.convertValue(nil)
			So(err, ShouldBeNil)
			So(val, ShouldBeNil)
		})
	})
}

func TestGetSchemaTableName(t *testing.T) {
	c := &Conn{
		p: &PluginParam{
			Schema: "config_schema",
			Table:  "config_table",
		},
	}
	
	Convey("Test getSchemaTableName", t, func() {
		data := &pluginDriver.PluginDataType{
			SchemaName: "data_schema",
			TableName:  "data_table",
		}

		Convey("Use config schema/table", func() {
			schema, table := c.getSchemaTableName(data)
			So(schema, ShouldEqual, "config_schema")
			So(table, ShouldEqual, "config_table")
		})

		Convey("Use data schema/table", func() {
			c.p.Schema = ""
			c.p.Table = ""
			schema, table := c.getSchemaTableName(data)
			So(schema, ShouldEqual, "data_schema")
			So(table, ShouldEqual, "data_table")
		})
	})
}

func TestGetParam(t *testing.T) {
	c := &Conn{}
	Convey("Test GetParam", t, func() {
		Convey("Valid param", func() {
			p := map[string]interface{}{
				"Schema":    "test_schema",
				"Table":     "test_table",
				"BatchSize": 1000,
			}
			param, err := c.GetParam(p)
			So(err, ShouldBeNil)
			So(param.Schema, ShouldEqual, "test_schema")
			So(param.Table, ShouldEqual, "test_table")
			So(param.BatchSize, ShouldEqual, 1000)
		})

		Convey("Default BatchSize", func() {
			p := map[string]interface{}{}
			param, err := c.GetParam(p)
			So(err, ShouldBeNil)
			So(param.BatchSize, ShouldEqual, 500)
		})
	})
}

func TestSendToCacheList(t *testing.T) {
	// Mock Conn to avoid real DB connection in AutoCommit
	c := &Conn{
		p: &PluginParam{
			BatchSize: 2,
			Data: &TableDataStruct{
				Data:       make([]*pluginDriver.PluginDataType, 0),
			},
		},
	}

	Convey("Test sendToCacheList", t, func() {
		data1 := &pluginDriver.PluginDataType{
			EventType: "insert",
			SchemaName: "db",
			TableName: "t1",
			Rows: []map[string]interface{}{
				{"id": 1},
			},
		}

		// First insert, should just append
		_, _, err := c.sendToCacheList(data1, false)
		So(err, ShouldBeNil)
		So(len(c.p.Data.Data), ShouldEqual, 1)

		// This test expects AutoCommit to fail because c.conn is nil, 
		// but we want to verify logic flow.
		// Since we cannot easily mock sql.DB, we expect it to panic or error out inside AutoCommit -> commitTableData -> c.conn.Begin()
		// However, c.conn is nil, so c.conn.Begin() will panic if not checked.
		// Let's check if we can make AutoCommit return early.
		// AutoCommit checks len(Data) == 0.
		
		// To test AutoCommit logic fully without DB, we'd need to abstract the DB layer interface.
		// For now, let's just ensure data is appended.
	})
}

// Mocking reflection test for completeness
func TestReflectLogic(t *testing.T) {
	c := &Conn{}
	Convey("Test reflect logic in convertValue fallback", t, func() {
		type MyStruct struct {
			Field string `json:"field"`
		}
		obj := MyStruct{Field: "hello"}
		val, err := c.convertValue(obj)
		So(err, ShouldBeNil)
		So(val, ShouldEqual, "{\"field\":\"hello\"}")
	})
}
