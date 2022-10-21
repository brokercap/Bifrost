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
	"fmt"
)

type DebeziumSchema struct {
	Type        string                 `json:"type"`
	Name        string                 `json:"name"`
	Namespace   string                 `json:"namespace"`
	Fields      []*DebeziumSchemaField `json:"fields"`
	ConnectName string                 `json:"connect.name"`
}

type DebeziumSchemaField struct {
	Type       string                 `json:"type"`
	Nullable   bool                   `json:"optional"`
	FieldName  string                 `json:"field"`
	Name       string                 `json:"name"`
	Default    interface{}            `json:"default,omitempty"`
	Parameters map[string]interface{} `json:"parameters"` // 这里采用 map[string]interface{} 而不是  map[string]string，是担心debezium有未知的字段采用非string的值进行存储，来个黑科技
}

type DebeziumKeyInfo struct {
	Schema  DebeziumSchema  `json:"schema"`
	Payload json.RawMessage `json:"payload"`
}

type DebeziumValueInfo struct {
	Schema  DebeziumValueSchema  `json:"schema"`
	Payload DebeziumValuePayload `json:"payload"`
}

type DebeziumValueSchema struct {
	Type        string           `json:"type"`
	Name        string           `json:"name"`
	Namespace   string           `json:"namespace"`
	Fields      []DebeziumSchema `json:"fields"`
	ConnectName string           `json:"connect.name"`
}

type DebeziumValuePayload struct {
	Before map[string]*json.RawMessage `json:"before"`
	After  map[string]*json.RawMessage `json:"after"`
	Source DebeziumValuePayloadSource  `json:"source"`

	Op string `json:"op"`
	Ts int64  `json:"ts_ms"`
	//Transaction *string `json:"transaction"`  //当前计算中不需要这个字段
}

// 因为source里的字段 取决于 debezium 中不同connector的插件信息，我们只用到通用的 name,db,table三个字段
type DebeziumValuePayloadSource struct {
	/*
	   "source": {
	       "version": "1.5.0.Final",
	       "connector": "mysql",
	       "name": "dbserver1",
	       "ts_ms": 1620714956000,
	       "snapshot": "false",
	       "db": "inventory",
	       "sequence": null,
	       "table": "products",
	       "server_id": 223344,
	       "gtid": null,
	       "file": "mysql-bin.000003",
	       "pos": 2820,
	       "row": 0,
	       "thread": null,
	       "query": null
	   },
	*/
	Name     string `json:"name"`   // 同步的名字，并不是插件名
	Database string `json:"db"`     // 数据库名
	Table    string `json:"table""` // 表名
}

type Debezium struct {
	Key   *DebeziumKeyInfo
	Value *DebeziumValueInfo
}

func NewDebeziumKeyInfo(key []byte) (*DebeziumKeyInfo, error) {
	if key == nil {
		return nil, nil
	}
	var data DebeziumKeyInfo
	err := json.Unmarshal(key, &data)
	if err != nil {
		return nil, err
	}
	return &data, err
}

func NewDebeziumValueInfo(value []byte) (*DebeziumValueInfo, error) {
	if value == nil {
		return nil, nil
	}
	var data DebeziumValueInfo
	err := json.Unmarshal(value, &data)
	if err != nil {
		return nil, err
	}
	return &data, err
}

func NewDebezium(key, value []byte) (*Debezium, error) {
	DebeziumKey, err := NewDebeziumKeyInfo(key)
	if err != nil {
		return nil, err
	}
	DebeziumValue, err := NewDebeziumValueInfo(value)
	if err != nil {
		return nil, err
	}
	return &Debezium{
		Key:   DebeziumKey,
		Value: DebeziumValue,
	}, nil
}

func (c *Debezium) ToBifrostOutputPluginData() *PluginDataType {
	return c.ToBifrostOutputPluginDataWithRow()
}

func (c *Debezium) GetPri() *[]string {
	if c.Key == nil {
		return nil
	}
	priArr := make([]string, 0)
	for _, v := range c.Key.Schema.Fields {
		priArr = append(priArr, v.FieldName)
	}
	return &priArr
}

func (c *Debezium) ToBifrostOutputPluginDataWithRow() *PluginDataType {
	var eventType string
	var rows []map[string]interface{}
	var columnMap map[string]string
	switch c.Value.Payload.Op {
	case "c", "r":
		eventType = "insert"
		rows, columnMap = c.GetToBifrostRowsWithInsert()
	case "u":
		eventType = "update"
		rows, columnMap = c.GetToBifrostRowsWithUpdate()
	case "d":
		eventType = "delete"
		rows, columnMap = c.GetToBifrostRowsWithDelete()
	case "t":
		return nil
	case "m":
		return nil
	default:
		return nil
	}
	data := &PluginDataType{
		EventType:     eventType,
		Rows:          rows,
		Pri:           *c.GetPri(),
		ColumnMapping: columnMap,
		SchemaName:    c.Value.Payload.Source.Database,
		TableName:     c.Value.Payload.Source.Table,
	}
	return data
}

func (c *Debezium) GetToBifrostRowsWithInsert() (rows []map[string]interface{}, columnMap map[string]string) {
	newDataMap, columnMap := c.GetToBifrostRowsAndMapping(c.Value.Payload.After, c.Value.Schema.Fields[1].Fields)
	rows = append(rows, newDataMap)
	return rows, columnMap
}

func (c *Debezium) GetToBifrostRowsWithDelete() (rows []map[string]interface{}, columnMap map[string]string) {
	newDataMap, columnMap := c.GetToBifrostRowsAndMapping(c.Value.Payload.Before, c.Value.Schema.Fields[0].Fields)
	rows = append(rows, newDataMap)
	return rows, columnMap
}

func (c *Debezium) GetToBifrostRowsWithUpdate() (rows []map[string]interface{}, columnMap map[string]string) {
	beforeMap, _ := c.GetToBifrostRowsAndMapping(c.Value.Payload.Before, c.Value.Schema.Fields[0].Fields)
	rows = append(rows, beforeMap)
	afterMap, columnMap := c.GetToBifrostRowsAndMapping(c.Value.Payload.Before, c.Value.Schema.Fields[0].Fields)
	rows = append(rows, afterMap)
	return rows, columnMap
}

func (c *Debezium) GetToBifrostRowsAndMapping(dataMap map[string]*json.RawMessage, fields []*DebeziumSchemaField) (newDataMap map[string]interface{}, columnMap map[string]string) {
	newDataMap = make(map[string]interface{}, len(dataMap))
	columnMap = make(map[string]string, len(dataMap))
	for _, v := range fields {
		name := v.FieldName
		var fieldType string = "text"
		var toVal interface{}
		var jsonRawMessage *json.RawMessage = dataMap[name]
		if jsonRawMessage != nil {
			toVal = string(*jsonRawMessage)
		}
		jsonRawMessageOjb := DebeziumJsonMsg{
			DebeziumParameters: v.Parameters,
			DebeziumVal:        toVal,
			DebeziumType:       v.Type,
		}
		switch v.Name {
		case "io.debezium.time.Timestamp":
			toVal, fieldType = jsonRawMessageOjb.ToBifrostTimestamp()
		case "io.debezium.time.ZonedTimestamp":
			toVal, fieldType = jsonRawMessageOjb.ToBifrostTimestamp()
		case "io.debezium.time.MicroTimestamp":
			toVal, fieldType = jsonRawMessageOjb.ToBifrostTimestamp()
		case "io.debezium.time.MicroTime":
			toVal, fieldType = jsonRawMessageOjb.ToBifrostTime()
		case "io.debezium.time.Date":
			toVal, fieldType = jsonRawMessageOjb.ToBifrostDate()
		case "io.debezium.time.Year":
			toVal, fieldType = jsonRawMessageOjb.ToBifrostYear()
		case "io.debezium.data.Json":
			toVal, fieldType = jsonRawMessageOjb.ToBifrostJson()
		case "io.debezium.data.Bits":
			toVal, fieldType = jsonRawMessageOjb.ToBifrostBits()
		case "org.apache.kafka.connect.data.Decimal":
			toVal, fieldType = jsonRawMessageOjb.ToBifrostDecimal()
		case "io.debezium.data.Enum":
			toVal, fieldType = jsonRawMessageOjb.ToBifrostEnum()
		case "io.debezium.data.EnumSet":
			toVal, fieldType = jsonRawMessageOjb.ToBifrostSet()

		default:
			switch v.Type {
			case "int64":
				toVal, fieldType = jsonRawMessageOjb.ToBifrostInt64()
			case "int32":
				toVal, fieldType = jsonRawMessageOjb.ToBifrostInt32()
			case "int16":
				toVal, fieldType = jsonRawMessageOjb.ToBifrostInt16()
			case "int8":
				toVal, fieldType = jsonRawMessageOjb.ToBifrostInt8()
			case "uint64":
				toVal, fieldType = jsonRawMessageOjb.ToBifrostUint64()
			case "uint32":
				toVal, fieldType = jsonRawMessageOjb.ToBifrostUint32()
			case "uint16":
				toVal, fieldType = jsonRawMessageOjb.ToBifrostUint16()
			case "uint8":
				toVal, fieldType = jsonRawMessageOjb.ToBifrostUint8()
			case "bytes":
				toVal, fieldType = jsonRawMessageOjb.ToBifrostLongText()
			default:
				toVal, fieldType = jsonRawMessageOjb.ToBifrostText()
			}
		}
		if v.Nullable {
			fieldType = fmt.Sprintf("Nullable(%s)", fieldType)
		}
		newDataMap[name] = toVal
		columnMap[name] = fieldType
	}
	return
}
