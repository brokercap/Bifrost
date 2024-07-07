package driver

import (
	"encoding/json"
	"reflect"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func getTestInsertMapJsonBytes() []byte {
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
	c, _ := json.Marshal(m)
	return c
}

func TestPluginDataCustomerJson_Decoder(t *testing.T) {
	c, _ := NewPluginDataCustomerJson()
	Convey("Decoder", t, func() {
		err := c.Decoder(getTestInsertMapJsonBytes())
		So(err, ShouldBeNil)
	})
}

func TestPluginDataCustomerJson_GetEventType(t *testing.T) {
	c, _ := NewPluginDataCustomerJson()
	Convey("GetEventType", t, func() {
		err := c.Decoder(getTestInsertMapJsonBytes())
		So(err, ShouldBeNil)
		c.SetEventTypePath([]string{"data", "eventType"})
		So(c.GetEventType(), ShouldEqual, "update")
	})
}

func TestPluginDataCustomerJson_GetInterfaceData(t *testing.T) {
	Convey("GetInterfaceData strings", t, func() {
		c, _ := NewPluginDataCustomerJson()
		err := c.Decoder(getTestInsertMapJsonBytes())
		So(err, ShouldBeNil)
		So(c.GetInterfaceData([]string{"a1"}), ShouldEqual, "a1")
	})

	Convey("GetInterfaceData map[string]interface{}", t, func() {
		c, _ := NewPluginDataCustomerJson()
		err := c.Decoder(getTestInsertMapJsonBytes())
		So(err, ShouldBeNil)
		So(reflect.TypeOf(c.GetInterfaceData([]string{"data", "newdata"})).Kind().String(), ShouldEqual, reflect.Map.String())
	})

	Convey("GetInterfaceData []interface{}", t, func() {
		c, _ := NewPluginDataCustomerJson()
		err := c.Decoder(getTestInsertMapJsonBytes())
		So(err, ShouldBeNil)
		So(reflect.TypeOf(c.GetInterfaceData([]string{"a4", "b4"})).Kind().String(), ShouldEqual, reflect.Slice.String())
	})
}

func TestPluginDataCustomerJson_GetMapData(t *testing.T) {
	c, _ := NewPluginDataCustomerJson()
	Convey("GetMapData normal", t, func() {
		err := c.Decoder(getTestInsertMapJsonBytes())
		So(err, ShouldBeNil)
		m := c.GetMapData([]string{"data", "newdata"})
		So(m["name"], ShouldEqual, "new_name")
	})

	Convey("GetMapData error", t, func() {
		err := c.Decoder(getTestInsertMapJsonBytes())
		So(err, ShouldBeNil)
		defer func() {
			var result bool = false
			if err := recover(); err != nil {
				result = true
			}
			So(result, ShouldEqual, true)
		}()
		c.GetMapData([]string{"a4", "b4"})
	})

	Convey("GetMapData nil", t, func() {
		err := c.Decoder(getTestInsertMapJsonBytes())
		So(err, ShouldBeNil)
		So(c.GetMapData(nil), ShouldBeNil)
	})
}

func TestPluginDataCustomerJson_GetPksData(t *testing.T) {
	Convey("[]string", t, func() {
		c, _ := NewPluginDataCustomerJson()
		m := map[string]interface{}{
			"a1": map[string]interface{}{
				"b1": []string{"s1", "s2"},
			},
		}
		c.SetPksPath([]string{"a1", "b1"})
		content, _ := json.Marshal(m)
		err := c.Decoder(content)
		So(err, ShouldBeNil)
		pks := c.GetPksData()
		So(pks[0], ShouldEqual, "s1")
		So(pks[1], ShouldEqual, "s2")
	})

	Convey("map[string]string", t, func() {
		c, _ := NewPluginDataCustomerJson()
		m := map[string]interface{}{
			"a1": map[string]interface{}{
				"b1": map[string]string{"s1": "s1_val", "s2": "s2_val"},
			},
		}
		c.SetPksPath([]string{"a1", "b1"})
		content, _ := json.Marshal(m)
		err := c.Decoder(content)
		So(err, ShouldBeNil)
		pks := c.GetPksData()
		So(pks[0], ShouldEqual, "s1")
		So(pks[1], ShouldEqual, "s2")
	})

	Convey("string", t, func() {
		c, _ := NewPluginDataCustomerJson()
		m := map[string]interface{}{
			"a1": map[string]interface{}{
				"b1": "s1",
			},
		}
		c.SetPksPath([]string{"a1", "b1"})
		content, _ := json.Marshal(m)
		err := c.Decoder(content)
		So(err, ShouldBeNil)
		pks := c.GetPksData()
		So(pks[0], ShouldEqual, "s1")
	})
}

func TestPluginDataCustomerJson_SetKey2Row(t *testing.T) {
	Convey("normal", t, func() {
		c, _ := NewPluginDataCustomerJson()
		var key2row []PluginCustomerJsonDataKey2Row
		key2row = append(key2row, PluginCustomerJsonDataKey2Row{
			Name: "testName",
			Path: []string{"a1", "b1"},
		})
		c.SetKey2Row(key2row)
		So(c.key2row[0].Name, ShouldEqual, "testName")
	})
}

func TestPluginDataCustomerJson_SetDatabasePath(t *testing.T) {
	Convey("normal", t, func() {
		c, _ := NewPluginDataCustomerJson()
		path := []string{"a1"}
		c.SetDatabasePath(path)
		So(c.databasePath, ShouldResemble, path)
	})
}

func TestPluginDataCustomerJson_SetTablePath(t *testing.T) {
	Convey("normal", t, func() {
		c, _ := NewPluginDataCustomerJson()
		path := []string{"a1"}
		c.SetTablePath(path)
		So(c.tablePath, ShouldResemble, path)
	})
}

func TestPluginDataCustomerJson_SetInsertDataPath(t *testing.T) {
	Convey("normal", t, func() {
		c, _ := NewPluginDataCustomerJson()
		path := []string{"a1"}
		c.SetInsertDataPath(path)
		So(c.insertDataPath, ShouldResemble, path)
	})
}

func TestPluginDataCustomerJson_SetUpdateNewDataPath(t *testing.T) {
	Convey("normal", t, func() {
		c, _ := NewPluginDataCustomerJson()
		path := []string{"a1"}
		c.SetUpdateNewDataPath(path)
		So(c.updateNewDataPath, ShouldResemble, path)
	})
}

func TestPluginDataCustomerJson_SetUpdateOldDataPath(t *testing.T) {
	Convey("normal", t, func() {
		c, _ := NewPluginDataCustomerJson()
		path := []string{"a1"}
		c.SetUpdateOldDataPath(path)
		So(c.UpdateOldDataPath, ShouldResemble, path)
	})
}

func TestPluginDataCustomerJson_SetDeleteDataPath(t *testing.T) {
	Convey("normal", t, func() {
		c, _ := NewPluginDataCustomerJson()
		path := []string{"a1"}
		c.SetDeleteDataPath(path)
		So(c.deleteDataPath, ShouldResemble, path)
	})
}

func TestPluginDataCustomerJson_SetPksPath(t *testing.T) {
	Convey("normal", t, func() {
		c, _ := NewPluginDataCustomerJson()
		path := []string{"a1"}
		c.SetPksPath(path)
		So(c.pksPath, ShouldResemble, path)
	})
}

func TestPluginDataCustomerJson_SetEventTypePath(t *testing.T) {
	Convey("normal", t, func() {
		c, _ := NewPluginDataCustomerJson()
		path := []string{"a1"}
		c.SetEventTypePath(path)
		So(c.eventTypePath, ShouldResemble, path)
	})
}

func TestPluginDataCustomerJson_SetEventTypeValInsert(t *testing.T) {
	Convey("normal", t, func() {
		c, _ := NewPluginDataCustomerJson()
		name := "i"
		c.SetEventTypeValInsert(name)
		So(c.eventTypeValInsert, ShouldEqual, name)
	})
}

func TestPluginDataCustomerJson_SetEventTypeValSelect(t *testing.T) {
	Convey("normal", t, func() {
		c, _ := NewPluginDataCustomerJson()
		name := "s"
		c.SetEventTypeValSelect(name)
		So(c.eventTypeValSelect, ShouldEqual, name)
	})
}

func TestPluginDataCustomerJson_SetEventTypeValUpdate(t *testing.T) {
	Convey("normal", t, func() {
		c, _ := NewPluginDataCustomerJson()
		name := "u"
		c.SetEventTypeValUpdate(name)
		So(c.eventTypeValUpdate, ShouldEqual, name)
	})
}

func TestPluginDataCustomerJson_SetEventTypeValDelete(t *testing.T) {
	Convey("normal", t, func() {
		c, _ := NewPluginDataCustomerJson()
		name := "d"
		c.SetEventTypeValDelete(name)
		So(c.eventTypeValDelete, ShouldEqual, name)
	})
}

func TestPluginDataCustomerJson_ToBifrostOutputPluginData(t *testing.T) {
	Convey("update", t, func() {
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
			"b3": map[string]interface{}{
				"id": 1,
			},
		}
		m["a4"] = map[string]interface{}{
			"b4": []string{"b4_val"},
		}
		m["schema"] = map[string]interface{}{
			"database": "databaseName",
			"table":    "tableName",
		}
		content, _ := json.Marshal(m)

		c, _ := NewPluginDataCustomerJson()
		err := c.Decoder(content)
		So(err, ShouldBeNil)
		c.SetUpdateOldDataPath([]string{"data", "olddata"})
		c.SetUpdateNewDataPath([]string{"data", "newdata"})
		c.SetEventTypeValUpdate("update")
		c.SetDatabasePath([]string{"schema", "database"})
		c.SetTablePath([]string{"schema", "table"})
		c.SetEventTypePath([]string{"data", "eventType"})
		newData := c.ToBifrostOutputPluginData()
		So(newData, ShouldNotBeNil)
		So(newData.EventType, ShouldEqual, "update")
		So(newData.Rows[1]["name"], ShouldEqual, "new_name")
		So(newData.Rows[0]["name"], ShouldEqual, "old_name")
		So(newData.SchemaName, ShouldEqual, "databaseName")
		So(newData.TableName, ShouldEqual, "tableName")
	})

	Convey("insert", t, func() {
		m := make(map[string]interface{}, 0)
		m["a1"] = "a1"
		m["data"] = map[string]interface{}{
			"eventType": "insert",
			"newdata": map[string]interface{}{
				"id":   1,
				"name": "new_name",
			},
		}
		content, _ := json.Marshal(m)

		c, _ := NewPluginDataCustomerJson()
		err := c.Decoder(content)
		So(err, ShouldBeNil)
		c.SetInsertDataPath([]string{"data", "newdata"})
		c.SetEventTypeValUpdate("insert")
		c.SetEventTypePath([]string{"data", "eventType"})
		newData := c.ToBifrostOutputPluginData()
		So(newData, ShouldNotBeNil)
		So(newData.EventType, ShouldEqual, "insert")
		So(newData.Rows[0]["name"], ShouldEqual, "new_name")
	})

	Convey("select", t, func() {
		m := make(map[string]interface{}, 0)
		m["a1"] = "a1"
		m["data"] = map[string]interface{}{
			"eventType": "select",
			"newdata": map[string]interface{}{
				"id":   1,
				"name": "new_name",
			},
		}
		content, _ := json.Marshal(m)

		c, _ := NewPluginDataCustomerJson()
		err := c.Decoder(content)
		So(err, ShouldBeNil)
		c.SetInsertDataPath([]string{"data", "newdata"})
		c.SetEventTypeValSelect("select")
		c.SetEventTypePath([]string{"data", "eventType"})
		newData := c.ToBifrostOutputPluginData()
		So(newData, ShouldNotBeNil)
		So(newData.EventType, ShouldEqual, "insert")
		So(newData.Rows[0]["name"], ShouldEqual, "new_name")
	})

	Convey("delete", t, func() {
		m := make(map[string]interface{}, 0)
		m["a1"] = "a1"
		m["data"] = map[string]interface{}{
			"eventType": "delete",
			"olddata": map[string]interface{}{
				"id":   1,
				"name": "old_name",
			},
		}
		content, _ := json.Marshal(m)

		c, _ := NewPluginDataCustomerJson()
		err := c.Decoder(content)
		So(err, ShouldBeNil)
		c.SetDeleteDataPath([]string{"data", "olddata"})
		c.SetEventTypeValDelete("delete")
		c.SetEventTypePath([]string{"data", "eventType"})
		newData := c.ToBifrostOutputPluginData()
		So(newData, ShouldNotBeNil)
		So(newData.EventType, ShouldEqual, "delete")
		So(newData.Rows[0]["name"], ShouldEqual, "old_name")
	})

	Convey("nil", t, func() {
		m := make(map[string]interface{}, 0)
		m["a1"] = "a1"
		m["data"] = map[string]interface{}{
			"eventType": "sql",
			"query":     "create table",
		}
		content, _ := json.Marshal(m)

		c, _ := NewPluginDataCustomerJson()
		err := c.Decoder(content)
		So(err, ShouldBeNil)
		c.SetInsertDataPath([]string{"data", "newdata"})
		c.SetEventTypePath([]string{"data", "eventType"})
		newData := c.ToBifrostOutputPluginData()
		So(newData, ShouldBeNil)
	})
}

func TestPluginDataCustomerJson_ToKey2Row(t *testing.T) {
	Convey("ToKey2Row", t, func() {
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
			"b3": map[string]interface{}{
				"id": 1,
			},
		}
		m["a4"] = map[string]interface{}{
			"b4":   []string{"b4_val"},
			"b4_2": "b4_2_val",
		}
		content, _ := json.Marshal(m)

		c, _ := NewPluginDataCustomerJson()
		err := c.Decoder(content)
		So(err, ShouldBeNil)
		var key2row []PluginCustomerJsonDataKey2Row
		key2row = append(key2row, PluginCustomerJsonDataKey2Row{
			Name: "testName",
			Path: []string{"a4", "b4_2"},
		})
		c.SetKey2Row(key2row)
		oldData := map[string]interface{}{
			"key": "old_val",
		}
		newData := map[string]interface{}{
			"key": "new_val",
		}
		rows := []map[string]interface{}{oldData, newData}
		data := &PluginDataType{
			Rows: rows,
		}
		c.ToKey2Row(data)
		So(data.Rows[1]["testName"], ShouldEqual, "b4_2_val")
	})
}

func TestPluginDataCustomerJson_ToBifrostUpdateRows(t *testing.T) {
	Convey("ToKey2Row", t, func() {
		m := make(map[string]interface{}, 0)
		m["a1"] = "a1"
		m["data"] = map[string]interface{}{
			"eventType": "update",
			"newdata": map[string]interface{}{
				"id":   1,
				"name": "new_name",
			},
		}
		content, _ := json.Marshal(m)
		c, _ := NewPluginDataCustomerJson()
		err := c.Decoder(content)
		So(err, ShouldBeNil)
		c.SetUpdateNewDataPath([]string{"data", "newdata"})
		rows := c.ToBifrostUpdateRows()
		So(rows[0]["name"], ShouldEqual, rows[1]["name"])
	})
}
