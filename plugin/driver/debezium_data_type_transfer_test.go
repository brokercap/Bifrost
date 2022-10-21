/*
Copyright [2018] [jc3wish]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package driver

import (
	"encoding/json"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func GetJsonRawMessage(m map[string]interface{}) map[string]*json.RawMessage {
	c, _ := json.Marshal(m)
	var data map[string]*json.RawMessage
	_ = json.Unmarshal(c, &data)
	return data
}

func TestDebeziumJsonMsg_ToBifrostTimestamp(t *testing.T) {
	m := map[string]interface{}{
		"int64_datetime6":  int64(1665857191098790),
		"int64_datetime":   int64(1665857191000000),
		"string_timestamp": "2022-10-15T10:06:31.09863Z",
	}
	data := GetJsonRawMessage(m)
	Convey("int64 datetime(6)", t, func() {
		jsonMsg := DebeziumJsonMsg{
			DebeziumVal:  string(*data["int64_datetime6"]),
			DebeziumType: "int64",
		}
		toVal, toType := jsonMsg.ToBifrostTimestamp()
		So(toType, ShouldEqual, "datetime(6)")
		So(toVal, ShouldEqual, "2022-10-15 18:06:31.098790")
	})

	Convey("int64 datetime", t, func() {
		jsonMsg := DebeziumJsonMsg{
			DebeziumVal:  string(*data["int64_datetime"]),
			DebeziumType: "int64",
		}
		toVal, toType := jsonMsg.ToBifrostTimestamp()
		So(toType, ShouldEqual, "datetime")
		So(toVal, ShouldEqual, "2022-10-15 18:06:31")
	})

	Convey("timestamp", t, func() {
		jsonMsg := DebeziumJsonMsg{
			DebeziumVal:  string(*data["string_timestamp"]),
			DebeziumType: "bytes",
		}
		toVal, toType := jsonMsg.ToBifrostTimestamp()
		So(toType, ShouldEqual, "timestamp(6)")
		So(toVal, ShouldEqual, "2022-10-15 10:06:31.09863")
	})

}

func TestDebeziumJsonMsg_ToBifrostEnum(t *testing.T) {
	Convey("enum", t, func() {
		m := map[string]interface{}{"testEnum": "en1"}
		data := GetJsonRawMessage(m)

		jsonMsg := DebeziumJsonMsg{
			DebeziumParameters: map[string]interface{}{"allowed": "en1,en2,en3"},
			DebeziumVal:        string(*data["testEnum"]),
			DebeziumType:       "string",
		}
		toVal, toType := jsonMsg.ToBifrostEnum()
		So(toType, ShouldEqual, "enum('en1','en2','en3')")
		So(toVal, ShouldEqual, "en1")
	})
}
