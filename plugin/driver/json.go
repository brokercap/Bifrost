package driver

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

func init() {
	gob.Register(map[string]interface{}{})
	gob.Register([]interface{}{})
	gob.Register(map[string][]map[string]interface{}{})
}

// 深度拷贝对象
func DeepCopy(dst, src interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		return err
	}
	return gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(dst)
}

type PluginDataTypeCopy PluginDataType

/*
deep copy data, then int32,uint64... to string
这里要进行 deepcopy 是防止同时多个地方在使用同一份数据
*/
func (c *PluginDataType) MarshalJSON() (b []byte, err error) {
	if c.Rows == nil || len(c.Rows) == 0 || c.ColumnMapping == nil {
		var cc *PluginDataTypeCopy
		cc = (*PluginDataTypeCopy)(c)
		return json.Marshal(cc)
	}
	var data PluginDataTypeCopy
	err = DeepCopy(&data, *c)
	if err != nil {
		return
	}
	var ok bool
	var mappingType string
	/*
		for _, row := range data.Rows {
			for key,val := range row {
				if val == nil {
					continue
				}
				if mappingType,ok = data.ColumnMapping[key];ok {
					switch mappingType {
					case "uint64","Nullable(uint64)":
						row[key] = fmt.Sprint(val)
					case "int64","Nullable(int64)":
						row[key] = fmt.Sprint(val)
					case "uint32","Nullable(uint32)":
						row[key] = fmt.Sprint(val)
					case "int32","Nullable(int32)","int24","Nullable(int24)":
						row[key] = fmt.Sprint(val)
					case "uint16","Nullable(uint16)":
						row[key] = fmt.Sprint(val)
					case "int16","Nullable(int16)","year(4)","Nullable(year(4))","year(2)","Nullable(year(2))":
						row[key] = fmt.Sprint(val)
					case "uint8","Nullable(uint8)":
						row[key] = fmt.Sprint(val)
					case "int8","Nullable(int8)":
						row[key] = fmt.Sprint(val)
					default:
						if strings.Index(mappingType,"bit") >= 0 {
							row[key] = fmt.Sprint(val)
							break
						}
						break
					}
				}
			}
		}
	*/

	var key string
	for key, mappingType = range data.ColumnMapping {
		switch mappingType {
		case "uint64", "Nullable(uint64)":
		case "int64", "Nullable(int64)":
		case "uint32", "Nullable(uint32)", "uint24", "Nullable(uint24)":
		case "int32", "Nullable(int32)", "int24", "Nullable(int24)":
		case "uint16", "Nullable(uint16)":
		case "int16", "Nullable(int16)", "year(4)", "Nullable(year(4))", "year(2)", "Nullable(year(2))":
		case "uint8", "Nullable(uint8)":
		case "int8", "Nullable(int8)":
		default:
			if strings.Index(mappingType, "bit") >= 0 {
				break
			}
			continue
		}
		var val interface{}
		for _, row := range data.Rows {
			if val, ok = row[key]; ok {
				row[key] = fmt.Sprint(val)
			}
		}
	}

	return json.Marshal(data)
}

/*
MarshalJSON 的时候，将 int32,uint64... to string 了
这里再将 对应的 string 转成 int32,uint64...
*/
func (c *PluginDataType) UnmarshalJSON(data []byte) error {
	var cc *PluginDataTypeCopy
	cc = (*PluginDataTypeCopy)(c)
	if err := json.Unmarshal(data, &cc); err != nil {
		return err
	}
	if cc.Rows == nil || len(cc.Rows) == 0 || cc.ColumnMapping == nil {
		return nil
	}
	var ok bool
	var mappingType string
	for _, row := range cc.Rows {
		for key, val := range row {
			if val == nil {
				continue
			}
			if mappingType, ok = cc.ColumnMapping[key]; ok {
				switch mappingType {
				case "uint64", "Nullable(uint64)":
					row[key], _ = strconv.ParseUint(fmt.Sprint(val), 10, 64)
				case "int64", "Nullable(int64)":
					row[key], _ = strconv.ParseInt(fmt.Sprint(val), 10, 64)
				case "uint32", "Nullable(uint32)":
					intA, _ := strconv.ParseUint(fmt.Sprint(val), 10, 32)
					row[key] = uint32(intA)
				case "int32", "Nullable(int32)", "int24", "Nullable(int24)":
					intA, _ := strconv.Atoi(fmt.Sprint(val))
					row[key] = int32(intA)
				case "uint16", "Nullable(uint16)":
					intA, _ := strconv.ParseUint(fmt.Sprint(val), 10, 32)
					row[key] = uint16(intA)
				case "int16", "Nullable(int16)", "year(4)", "Nullable(year(4))", "year(2)", "Nullable(year(2))":
					intA, _ := strconv.Atoi(fmt.Sprint(val))
					row[key] = int16(intA)
				case "uint8", "Nullable(uint8)":
					intA, _ := strconv.Atoi(fmt.Sprint(val))
					row[key] = uint8(intA)
				case "int8", "Nullable(int8)":
					intA, _ := strconv.Atoi(fmt.Sprint(val))
					row[key] = int8(intA)
				case "float32":
					switch val.(type) {
					case float64:
						row[key] = float32(val.(float64))
					default:
						break
					}
				case "float64":
					switch val.(type) {
					case float32:
						row[key] = float64(val.(float32))
					default:
						row[key], _ = strconv.ParseFloat(fmt.Sprint(val), 64)
					}
					break
				default:
					if strings.Index(mappingType, "double") >= 0 {
						switch val.(type) {
						case float32:
							row[key] = float64(val.(float32))
						default:
							row[key], _ = strconv.ParseFloat(fmt.Sprint(val), 64)
						}
						break
					}
					if strings.Index(mappingType, "float64") >= 0 {
						switch val.(type) {
						case float32:
							row[key] = float64(val.(float32))
						default:
							row[key], _ = strconv.ParseFloat(fmt.Sprint(val), 64)
						}
						break
					}
					if strings.Index(mappingType, "float") >= 0 {
						switch val.(type) {
						case float64:
							row[key] = float32(val.(float64))
						default:
							f1, _ := strconv.ParseFloat(fmt.Sprint(val), 64)
							row[key] = float32(f1)
						}
						break
					}
					if strings.Index(mappingType, "bit") >= 0 {
						switch val.(type) {
						case float64:
							row[key] = int64(val.(float64))
						case float32:
							row[key] = int64(val.(float32))
						default:
							row[key], _ = strconv.ParseInt(fmt.Sprint(val), 10, 64)
						}
						break
					}
					break
				}
			}
		}
	}
	return nil
}
