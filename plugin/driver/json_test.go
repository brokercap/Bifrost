package driver

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestPluginDataType_MarshalJSON(t *testing.T) {
	row0 := make(map[string]interface{}, 0)
	row0["id"] = uint64(1)
	row0["int64"] = int64(9007199254740992)
	row0["uint64"] = uint64(9007199254740993)
	row0["float32"] = float32(8823.22)
	row0["float64"] = float64(98823.22)
	row0["string"] = "sdfsdfsdf中国人"
	row0["set"] = []string{"aa", "bb"}

	row1 := make(map[string]interface{}, 0)
	row1["id"] = uint64(2)
	row1["int64"] = int64(-99999)
	row1["uint64"] = uint64(9999999999)
	row1["float32"] = float32(8823.22)
	row1["float64"] = float64(98823.22)
	row1["string"] = "中国人sdfsdfsdf"
	row1["set"] = []string{"aa", "cc"}

	ColumnMapping := make(map[string]string, 0)
	ColumnMapping["id"] = "uint64"
	ColumnMapping["int64"] = "int64"
	ColumnMapping["uint64"] = "Nullable(uint64)"
	ColumnMapping["float32"] = "float"
	ColumnMapping["float64"] = "double"
	ColumnMapping["string"] = "varchar(200)"
	ColumnMapping["set"] = "set(\"aa\",\"bb\",\"cc\")"

	data0 := &PluginDataType{
		Timestamp:      uint32(time.Now().Unix()),
		EventType:      "insert",
		Rows:           []map[string]interface{}{row0, row1},
		Query:          "",
		SchemaName:     "bifrost_test",
		TableName:      "json_test",
		BinlogFileNum:  0,
		BinlogPosition: 0,
		ColumnMapping:  ColumnMapping,
	}

	c, err := json.Marshal(data0)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(c))

	var data2 PluginDataType
	err = json.Unmarshal(c, &data2)
	if err != nil {
		t.Fatal(err)
	}

	for i, row := range data0.Rows {
		for k, v := range row {
			switch reflect.ValueOf(v).Kind() {
			case reflect.Slice, reflect.Map:
				if fmt.Sprint(v) != fmt.Sprint(data2.Rows[i][k]) {
					t.Fatal("i:", i, " key:", k, " old val: ", v, "!=  new val:", data2.Rows[i][k])
				}
			default:
				if reflect.ValueOf(v).Kind() != reflect.ValueOf(data2.Rows[i][k]).Kind() || fmt.Sprint(v) != fmt.Sprint(data2.Rows[i][k]) {
					t.Fatal("i:", i, " key:", k, " old val: ", v, "(", reflect.ValueOf(v).Kind(), ") !=  new val:", data2.Rows[i][k], "(", reflect.ValueOf(data2.Rows[i][k]).Kind(), ")")
				}
			}
			t.Log("i:", i, " key:", k, " old val: ", v, " ==  new val:", data2.Rows[i][k])
		}
	}

	t.Log("success")
}

func TestPluginDataType_MarshalJSON2(t *testing.T) {
	ColumnMapping := make(map[string]string, 0)
	ColumnMapping["id"] = "uint64"
	ColumnMapping["int64"] = "int64"
	ColumnMapping["uint64"] = "Nullable(uint64)"
	ColumnMapping["int32"] = "Nullable(int32)"
	ColumnMapping["float32"] = "float"
	ColumnMapping["float64"] = "double"
	ColumnMapping["string"] = "varchar(200)"
	ColumnMapping["set"] = "set(\"aa\",\"bb\",\"cc\")"

	data0 := &PluginDataType{
		Timestamp:      uint32(time.Now().Unix()),
		EventType:      "insert",
		Rows:           make([]map[string]interface{}, 0),
		Query:          "",
		SchemaName:     "bifrost_test",
		TableName:      "json_test",
		BinlogFileNum:  0,
		BinlogPosition: 0,
		ColumnMapping:  ColumnMapping,
	}

	for i := 0; i < 1; i++ {
		row0 := make(map[string]interface{}, 0)
		row0["id"] = uint64(i)
		row0["int64"] = int64(9007199254740992)
		row0["uint64"] = uint64(9007199254740993)
		row0["int32"] = int32(5621851)
		row0["float32"] = float32(8823.22)
		row0["float64"] = float64(98823.22)
		row0["string"] = "sdfsdfsdf中国人"
		row0["set"] = []string{"aa", "bb"}
		row0["set"] = []string{"aa", "bb"}
		row0["set"] = []string{"aa", "bb"}
		row0["set"] = []string{"aa", "bb"}
		row0["set"] = []string{"aa", "bb"}
		row0["set"] = []string{"aa", "bb"}
		row0["set"] = []string{"aa", "bb"}
		data0.Rows = append(data0.Rows, row0)
	}

	startTime := time.Now().UnixNano()
	for i := 0; i < 10000; i++ {
		json.Marshal(data0)
	}
	endTime := time.Now().UnixNano()

	t.Logf("use time: %d ms", (endTime-startTime)/1e6)

}

func TestPluginDataType_MarshalJSON3(t *testing.T) {
	data0 := &PluginDataType{
		Timestamp:      uint32(time.Now().Unix()),
		EventType:      "commit",
		Rows:           nil,
		Query:          "COMMIT",
		SchemaName:     "bifrost_test",
		TableName:      "json_test",
		BinlogFileNum:  0,
		BinlogPosition: 0,
		ColumnMapping:  nil,
	}
	b, err := json.Marshal(data0)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(b))
}

func TestDeepCopy(t *testing.T) {
	row0 := make(map[string]interface{}, 0)
	row0["id"] = uint64(1)
	row0["int64"] = int64(9007199254740992)
	row0["uint64"] = uint64(9007199254740993)
	row0["float32"] = float32(8823.22)
	row0["float64"] = float64(98823.22)
	row0["string"] = "sdfsdfsdf中国人"
	row0["set"] = []string{"aa", "bb"}

	ColumnMapping := make(map[string]string, 0)
	ColumnMapping["id"] = "uint64"
	ColumnMapping["int64"] = "int64"
	ColumnMapping["uint64"] = "Nullable(uint64)"
	ColumnMapping["float32"] = "float"
	ColumnMapping["float64"] = "double"
	ColumnMapping["string"] = "varchar(200)"
	ColumnMapping["set"] = "set(\"aa\",\"bb\",\"cc\")"

	data0 := &PluginDataType{
		Timestamp:      uint32(time.Now().Unix()),
		EventType:      "insert",
		Rows:           []map[string]interface{}{row0},
		Query:          "",
		SchemaName:     "bifrost_test",
		TableName:      "json_test",
		BinlogFileNum:  0,
		BinlogPosition: 0,
		ColumnMapping:  ColumnMapping,
	}

	_, err := json.Marshal(data0)
	if err != nil {
		t.Fatal(err)
	}

	switch data0.Rows[0]["id"].(type) {
	case uint64:
		t.Log("id type uint64", "success")
		break
	default:
		t.Fatal("id type uint64 != ", reflect.ValueOf(data0.Rows[0]["id"]).Kind())
	}

	t.Log(data0)

	t.Log("success")
}
