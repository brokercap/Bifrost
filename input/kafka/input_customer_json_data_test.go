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
package kafka

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"

	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
)

func getInitParamMap() map[string]string {
	ParamMap := make(map[string]string, 0)
	ParamMap["input.key2row"] = "a1.b1:test_name,a2.b2.c2:test_name2"
	ParamMap["input.database"] = "database"
	ParamMap["input.table"] = "table"
	ParamMap["input.pks"] = "a4.b4"
	ParamMap["input.update_new_data"] = "data.new_data"
	ParamMap["input.update_old_data"] = "data.old_data"
	ParamMap["input.insert_data"] = "data.insert_data"
	ParamMap["input.delete_data"] = "data.delete_data"
	ParamMap["input.event.type"] = "event_type"
	ParamMap["input.event.type.val.insert"] = "val_insert"
	ParamMap["input.event.type.val.select"] = "val_select"
	ParamMap["input.event.type.val.update"] = "val_update"
	ParamMap["input.event.type.val.delete"] = "val_delete"
	return ParamMap
}

func TestCustomerJsonDataInput_TransferConfig(t *testing.T) {
	c := NewCustomerJsonDataInput0()
	c.config = &Config{
		ParamMap: getInitParamMap(),
	}
}

func TestCustomerJsonDataInput_getConfig(t *testing.T) {
	c := NewCustomerJsonDataInput0()
	c.config = &Config{
		ParamMap: getInitParamMap(),
	}
	Convey("normal", t, func() {
		eventType := c.getConfig("input.event.type")
		So(*eventType, ShouldEqual, "event_type")
	})
	Convey("no_key", t, func() {
		So(c.getConfig("no_key"), ShouldBeNil)
	})
}

func TestCustomerJsonDataInput_tansferConfigKey2Row(t *testing.T) {
	Convey("nil", t, func() {
		c := NewCustomerJsonDataInput0()
		result := c.tansferConfigPath(nil)
		So(result, ShouldBeNil)
	})
	Convey("noraml", t, func() {
		c := NewCustomerJsonDataInput0()
		c.config = &Config{
			ParamMap: map[string]string{
				"input.key2row": "a1.b1:test_name,a3.b3:name1:name2,test_name3",
			},
		}
		key2rowPath := c.tansferConfigKey2Row(c.getConfig("input.key2row"))
		So(len(key2rowPath), ShouldEqual, 2)
		So(key2rowPath[0].Name, ShouldEqual, "test_name")
		So(key2rowPath[1].Name, ShouldEqual, "test_name3")
	})
}

func TestCustomerJsonDataInput_tansferConfigPath(t *testing.T) {
	c := NewCustomerJsonDataInput0()
	c.config = &Config{
		ParamMap: getInitParamMap(),
	}
	Convey("normal", t, func() {
		pathStr := "input.event.event"
		list := c.tansferConfigPath(&pathStr)
		So(len(list), ShouldEqual, 3)
		So(list[0], ShouldEqual, "input")
		So(list[1], ShouldEqual, "event")
		So(list[2], ShouldEqual, "event")
	})
	Convey("nil", t, func() {
		So(c.tansferConfigPath(nil), ShouldBeNil)
	})
}

func TestCustomerJsonDataInput_CallBack(t *testing.T) {
	Convey("callback nil", t, func() {
		c := NewCustomerJsonDataInput0()
		c.config = &Config{
			ParamMap: getInitParamMap(),
		}
		c.callback = nil
		err := c.CallBack(nil)
		So(err, ShouldBeNil)
	})

	Convey("callback normal", t, func() {
		var callbackData = make([]*pluginDriver.PluginDataType, 0)
		var f = func(data *pluginDriver.PluginDataType) {
			callbackData = append(callbackData, data)
			return
		}
		c := NewCustomerJsonDataInput0()
		c.config = &Config{
			ParamMap: getInitParamMap(),
		}
		c.callback = f

		m := make(map[string]interface{}, 0)
		m["a1"] = "a1"
		m["data"] = map[string]interface{}{
			"eventType": "update",
			"newdata": map[string]interface{}{
				"id":   1,
				"name": "new_name",
			},
			"olddata": map[string]interface{}{
				"id":   1,
				"name": "old_name",
			},
		}
		m["a3"] = map[string]interface{}{
			"pks": map[string]interface{}{
				"id": 1,
			},
		}
		m["a4"] = map[string]interface{}{
			"b4": []string{"b4_val"},
		}
		content, _ := json.Marshal(m)

		ParamMap := make(map[string]string, 0)
		ParamMap["input.key2row"] = "a1.b1:test_name,a2.b2.c2:test_name2"
		ParamMap["input.database"] = "database"
		ParamMap["input.table"] = "table"
		ParamMap["input.pks"] = "a4.b4"
		ParamMap["input.update_new_data"] = "data.newdata"
		ParamMap["input.update_old_data"] = "data.olddata"
		ParamMap["input.insert_data"] = "data.insert_data"
		ParamMap["input.delete_data"] = "data.delete_data"
		ParamMap["input.event.type"] = "data.eventType"
		c.config = &Config{ParamMap: ParamMap}

		c.CallBack(&sarama.ConsumerMessage{Value: content})
		So(callbackData[0].EventType, ShouldEqual, "update")
	})

	Convey("callback decoder err", t, func() {
		var callbackData = make([]*pluginDriver.PluginDataType, 0)
		var f = func(data *pluginDriver.PluginDataType) {
			callbackData = append(callbackData, data)
			return
		}
		c := NewCustomerJsonDataInput0()
		c.config = &Config{
			ParamMap: getInitParamMap(),
		}
		c.callback = f
		err := c.CallBack(&sarama.ConsumerMessage{Value: []byte("fffffffffff")})
		So(err, ShouldNotBeNil)
	})

	Convey("ToBifrostOutputPluginData nil", t, func() {
		var callbackData = make([]*pluginDriver.PluginDataType, 0)
		var f = func(data *pluginDriver.PluginDataType) {
			callbackData = append(callbackData, data)
			return
		}
		c := NewCustomerJsonDataInput0()
		c.config = &Config{
			ParamMap: getInitParamMap(),
		}
		c.callback = f

		m := make(map[string]interface{}, 0)
		m["data"] = map[string]interface{}{
			"eventType": "other",
			"newdata": map[string]interface{}{
				"id":   1,
				"name": "new_name",
			},
			"olddata": map[string]interface{}{
				"id":   1,
				"name": "old_name",
			},
		}
		content, _ := json.Marshal(m)

		patches := gomonkey.ApplyMethod(reflect.TypeOf(c.pluginCustomerDataObj), "ToBifrostOutputPluginData", func() *pluginDriver.PluginDataType {
			return nil
		})
		defer patches.Reset()
		err := c.CallBack(&sarama.ConsumerMessage{Value: content})
		So(err, ShouldBeNil)
		So(len(callbackData), ShouldEqual, 0)
	})
}
