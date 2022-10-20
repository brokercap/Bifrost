package driver

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
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
		if jsonRawMessage == nil {
			toVal = nil
		} else {
			toVal = string(*jsonRawMessage)
		}
		switch v.Type {
		case "int64":
			if toVal != nil {
				toVal, _ = strconv.ParseInt(toVal.(string), 10, 64)
			}
			fieldType = "int64"
		case "int32":
			if toVal != nil {
				intA, _ := strconv.ParseInt(toVal.(string), 10, 32)
				toVal = int32(intA)
			}
			fieldType = "int32"
		case "int16":
			if toVal != nil {
				intA, _ := strconv.Atoi(toVal.(string))
				toVal = int16(intA)
			}
			fieldType = "int16"
		case "int8":
			if toVal != nil {
				intA, _ := strconv.Atoi(toVal.(string))
				toVal = int8(intA)
			}
			fieldType = "int8"
		case "uint64":
			if toVal != nil {
				toVal, _ = strconv.ParseUint(toVal.(string), 10, 64)
			}
			fieldType = "uint64"
		case "uint32":
			if toVal != nil {
				intA, _ := strconv.ParseUint(toVal.(string), 10, 32)
				toVal = uint32(intA)
			}
			fieldType = "uint32"
		case "uint16":
			if toVal != nil {
				intA, _ := strconv.Atoi(toVal.(string))
				toVal = uint16(intA)
			}
			fieldType = "uint16"
		case "uint8":
			if toVal != nil {
				intA, _ := strconv.Atoi(toVal.(string))
				toVal = uint8(intA)
			}
			fieldType = "uint8"
		case "float":
			if toVal != nil {
				float64Val, _ := strconv.ParseFloat(toVal.(string), 32)
				toVal = float32(float64Val)
			}
			fieldType = "float"
		case "double":
			if toVal != nil {
				toVal, _ = strconv.ParseFloat(toVal.(string), 64)
			}
			fieldType = "double"
		case "bytes":
			switch v.Name {
			case "org.apache.kafka.connect.data.Decimal":
				var scale interface{}
				if v.Parameters != nil {
					if _, ok := v.Parameters["scale"]; ok {
						scale = v.Parameters["scale"]
					} else {
						// 假如没有 scale 则说明没有 precision,scale
						fieldType = "decimal"
						break
					}
					fieldType = fmt.Sprintf("decimal(%s,%s)", v.Parameters["connect.decimal.precision"], scale)
				} else {
					fieldType = "decimal"
				}
				// string(json.RawMessage) 假如是字符串的情况下 是会前后增加引号的，比如 空字符串， string(json.RawMessage) == "\"\"" ,有个引号
				if toVal != nil {
					toVal, _ = strconv.Unquote(toVal.(string))
				}
				break
			case "io.debezium.time.Timestamp":
				if toVal != nil {
					// 这里不进行 time.Parse("2006-01-02 15:04:05", v) 的方式，是因为这里没办法区分 2006-01-02 15:04:05.999999 小数点后到底有几位等操作，下同
					toVal = c.TransToGoTimestampFormatStr(toVal.(string))
				}
				break
			case "io.debezium.data.Bits":
				if toVal != nil {
					toVal, _ = strconv.ParseInt(toVal.(string), 10, 64)
				}
				fieldType = "bit"
			default:
				if toVal != nil {
					toVal, _ = strconv.Unquote(toVal.(string))
				}
				fieldType = "text"
			}
		default:
			switch v.Name {
			//case "io.debezium.data.Enum", "io.debezium.data.EnumSet":
			//	fieldType = "text"
			case "io.debezium.time.ZonedTimestamp":
				if toVal != nil {
					toVal = c.TransToGoTimestampFormatStr(toVal.(string))
				}
				fieldType = "timestamp"
			case "io.debezium.data.Json":
				fieldType = "json"
				if toVal != nil {
					// string(json.RawMessage) 出来结果如下：
					// "{\"key1\":[2147483647,-2147483648,\"2\",null,true,922337203685477,-922337203685477,{\"key2\":\"qoY`uY,Np5Q\\\\OpX9&'o8试测测试据试数数数试试测据试测测\"},{\"key2\":false}]}"
					//toVal = strings.ReplaceAll(strings.Trim(toVal.(string), "\""), "\\\"", "\"")
					toVal, _ = strconv.Unquote(toVal.(string))
				}
			default:
				fieldType = "text"
				if toVal != nil {
					toVal, _ = strconv.Unquote(toVal.(string))
				}
			}
			break
		}
		if v.Nullable {
			fieldType = fmt.Sprintf("Nullable(%s)", fieldType)
		}
		newDataMap[name] = toVal
		columnMap[name] = fieldType
	}
	return
}

func (c *Debezium) TransToGoTimestampFormatStr(str string) string {
	return strings.ReplaceAll(strings.ReplaceAll(strings.Trim(str, "\""), "T", " "), "Z", "")
}
