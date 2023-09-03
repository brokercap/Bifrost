package driver

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestPluginDataType_ToTableMapObject(t *testing.T) {
	Convey("Bifrost转TableMap", t, func() {
		var maxUint64Interface interface{}
		var maxUint64 = uint64(math.MaxUint64)
		maxUint64Interface = maxUint64
		c := &PluginDataType{
			Timestamp:       0,
			EventSize:       0,
			EventType:       "insert",
			Rows:            []map[string]interface{}{{"int64": int64(math.MaxInt64), "uint64": maxUint64Interface, "int64_str": fmt.Sprintf("%d", int64(math.MaxInt64)), "uint64_str": fmt.Sprintf("%d", uint64(math.MaxUint64)), "bool": true, "key": "1111"}},
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
		So(dataMap["int64"], ShouldEqual, fmt.Sprintf("%d", int64(math.MaxInt64)))
		So(dataMap["uint64"], ShouldEqual, fmt.Sprintf("%d", uint64(math.MaxUint64)))
		So(dataMap["int64_str"], ShouldEqual, fmt.Sprintf("%d", int64(math.MaxInt64)))
		So(dataMap["uint64_str"], ShouldEqual, fmt.Sprintf("%d", uint64(math.MaxUint64)))
		So(dataMap["bool"], ShouldEqual, "true")
		So(dataMap["key"], ShouldEqual, "1111")
		So(dataMap["bifrost_pri"], ShouldEqual, "id")
		So(dataMap["bifrost_database"], ShouldEqual, "database")
		So(dataMap["bifrost_table"], ShouldEqual, "table")
		So(dataMap["binlog_event_type"], ShouldEqual, "insert")

		body, err := json.Marshal(dataMap)

		So(err, ShouldBeNil)
		t.Log(string(body))

		c.Rows = nil
		c.Query = "select 1"
		c.EventType = "sql"

		dataMap, _ = c.ToTableMapObject()
		So(dataMap["bifrost_query"], ShouldEqual, c.Query)

	})
}
