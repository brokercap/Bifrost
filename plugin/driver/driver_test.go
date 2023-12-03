package driver

import (
	"encoding/json"
	"fmt"
	"github.com/smartystreets/goconvey/convey"
	"regexp"
	"strconv"
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
		Gtid:           "gtidTest",
	}
	convey.Convey("normal", t, func() {
		r1 := TransfeResult("{$json1[json]['0']['testkey']}", data, 0)
		convey.So(r1, convey.ShouldEqual, "testVal")

		b, _ := json.Marshal(data)
		var data2 PluginDataType
		err := json.Unmarshal(b, &data2)
		if err != nil {
			t.Fatal(err)
		}
		r10 := TransfeResult("{$json1[json]['0']['testkey']}", &data2, 0)
		convey.So(r10, convey.ShouldEqual, "testVal")

		convey.So(TransfeResult("{$key1}", data, 0), convey.ShouldEqual, "key1")
		convey.So(TransfeResult("{$SchemaName}", data, 0), convey.ShouldEqual, "bifrost_test")
		convey.So(TransfeResult("{$TableName}", data, 0), convey.ShouldEqual, "bifrost_test_table")
		convey.So(TransfeResult("{$EventType}", data, 0), convey.ShouldEqual, data.EventType)
		convey.So(TransfeResult("{$BinlogTimestamp}", data, 0), convey.ShouldEqual, fmt.Sprint(data.Timestamp))
		convey.So(TransfeResult("{$BinlogDateTime}", data, 0), convey.ShouldEqual, time.Unix(int64(data.Timestamp), 0).Format("2006-01-02 15:04:05"))
		convey.So(TransfeResult("{$BinlogFileNum}", data, 0), convey.ShouldEqual, "1")
		convey.So(TransfeResult("{$BinlogPosition}", data, 0), convey.ShouldEqual, "1000")
		convey.So(TransfeResult("{$GTID}", data, 0), convey.ShouldEqual, data.Gtid)
		convey.So(TransfeResult("{$BifrostNull}", data, 0), convey.ShouldBeNil)

		nowTimeString := TransfeResult("{$Timestamp}", data, 0)
		nowTimeInt64, err := strconv.ParseInt(fmt.Sprint(nowTimeString), 10, 64)
		convey.So(err, convey.ShouldBeNil)
		nowTime := time.Unix(nowTimeInt64, 0)
		convey.So(nowTime.IsZero(), convey.ShouldEqual, false)

		tags := "{$SchemaName}-{$TableName}-{$json1[json]['0']['testkey']}-{$BifrostNull}"

		newVal := TransfeResult(tags, data, 0)
		needNewVal := fmt.Sprintf("%s-%s-%s-", data.SchemaName, data.TableName, jsonData["json"][0]["testkey"])
		convey.So(newVal, convey.ShouldEqual, needNewVal)

	})

	convey.Convey("data is nil", t, func() {
		r1 := TransfeResult("{$json1[json]['0']['testkey']}", nil, 0)
		convey.So(r1, convey.ShouldBeNil)
	})

	convey.Convey("noTagsReturnNil", t, func() {
		r1 := TransfeResult("noTags", data, 0, true)
		convey.So(r1, convey.ShouldBeNil)
	})

	convey.Convey("noTagsReturnNil == false", t, func() {
		r1 := TransfeResult("noTags", data, 0)
		convey.So(r1, convey.ShouldEqual, "noTags")
	})

	convey.Convey("tags not be found ,data is normal", t, func() {
		r1 := TransfeResult("{$keyNotBeFound0}", data, 0)
		convey.So(r1, convey.ShouldEqual, "{$keyNotBeFound0}")

		r2 := TransfeResult("{$keyNotBeFound0}-{$keyNotBeFound1}", data, 0)
		convey.So(r2, convey.ShouldEqual, "{$keyNotBeFound0}-{$keyNotBeFound1}")
	})

	convey.Convey("tags not be found and rows is nil", t, func() {
		data.Rows = nil
		r1 := TransfeResult("{$keyNotBeFound0}", data, 0)
		convey.So(r1, convey.ShouldEqual, nil)

		r2 := TransfeResult("{$keyNotBeFound0}-{$keyNotBeFound1}", data, 0)
		convey.So(r2, convey.ShouldEqual, "<nil>-<nil>")
	})
}

func TestReg(t *testing.T) {
	var RegularxEpressionKey = `([a-zA-Z0-9\-\_]+)`
	reqTagKey, _ := regexp.Compile(RegularxEpressionKey)
	v := "{$testjson['key4']['nkey3'] [1] }"
	p2 := reqTagKey.FindAllStringSubmatch(v, -1)
	t.Log(p2)

	p := reqTagAll.FindAllStringSubmatch("$id", -1)
	t.Log(p)
}
