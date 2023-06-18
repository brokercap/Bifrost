package driver

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestPluginDataType_ToTableMapObject(t *testing.T) {
	Convey("Bifrost转TableMap", t, func() {
		c := &PluginDataType{
			Timestamp:       0,
			EventSize:       0,
			EventType:       "insert",
			Rows:            []map[string]interface{}{{"int64": int64(1), "uint64": uint64(2), "bool": true, "key": "1111"}},
			Query:           "",
			SchemaName:      "database",
			TableName:       "table",
			AliasSchemaName: "",
			AliasTableName:  "",
			BinlogFileNum:   0,
			BinlogPosition:  0,
			Gtid:            "",
			Pri:             []string{"id"},
			EventID:         0,
			ColumnMapping:   nil,
		}
		dataMap, _ := c.ToTableMapObject()
		So(dataMap["int64"], ShouldEqual, "1")
		So(dataMap["uint64"], ShouldEqual, "2")
		So(dataMap["bool"], ShouldEqual, "true")
		So(dataMap["key"], ShouldEqual, "1111")
		So(dataMap["bifrost_pri"], ShouldEqual, "id")
		So(dataMap["bifrost_database"], ShouldEqual, "database")
		So(dataMap["bifrost_table"], ShouldEqual, "table")
		So(dataMap["binlog_event_type"], ShouldEqual, "insert")

		c.Rows = nil
		c.Query = "select 1"
		c.EventType = "sql"

		dataMap, _ = c.ToTableMapObject()
		So(dataMap["bifrost_query"], ShouldEqual, c.Query)
	})
}
