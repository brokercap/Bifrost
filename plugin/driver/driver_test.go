package driver

import (
	"encoding/json"
	"fmt"
	"regexp"
	"testing"
	"time"
)

func TestTransfeResult(t *testing.T) {
	jsonData := make(map[string][]map[string]string)
	jsonData["json"] = make([]map[string]string, 1)
	jsonData["json"][0] = make(map[string]string, 0)
	jsonData["json"][0]["testkey"] = "testVal"
	id := "id"
	Pri := make([]string, 1)
	Pri[0] = id
	Row := make(map[string]interface{}, 0)
	Row["id"] = 1
	Row["key1"] = "key1"
	Row["key2"] = "key2"
	Row["json1"] = jsonData
	data := &PluginDataType{
		Timestamp:      uint32(time.Now().Unix()),
		EventType:      "insert",
		Rows:           []map[string]interface{}{Row},
		Query:          "",
		SchemaName:     "bifrost_test",
		TableName:      "bifrost_test_table",
		BinlogFileNum:  1,
		BinlogPosition: 1000,
		Pri:            Pri,
	}

	r1 := TransfeResult("{$json1[json]['0']['testkey']}", data, 0)
	if fmt.Sprint(r1) == "testVal" {
		t.Log("success")
	} else {
		t.Fatal("r1:", fmt.Sprint(r1), " != ", "testVal")
	}

	b, _ := json.Marshal(data)
	var data2 PluginDataType
	err := json.Unmarshal(b, &data2)
	if err != nil {
		t.Fatal(err)
	}

	r2 := TransfeResult("{$json1[json]['0']['testkey']}", &data2, 0)
	if fmt.Sprint(r2) == "testVal" {
		t.Log("success")
	} else {
		t.Fatal("r1:", fmt.Sprint(r2), " != ", "testVal")
	}
}

func TestReg(t *testing.T) {
	var RegularxEpressionKey = `([a-zA-Z0-9\-\_]+)`
	reqTagKey, _ := regexp.Compile(RegularxEpressionKey)
	v := "{$testjson['key4']['nkey3'] [1] }"
	p2 := reqTagKey.FindAllStringSubmatch(v, -1)
	t.Log(p2)

}
