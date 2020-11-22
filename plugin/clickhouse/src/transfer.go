package src

import (
	"strings"
	"time"
	"reflect"
	"fmt"
	"strconv"
	"encoding/json"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
)

func AllTypeToInt64(s interface{}) (int64, error) {
	t := strings.Trim(fmt.Sprint(s), " ")
	t = strings.Trim(t, "　")
	i64, err := strconv.ParseInt(t, 10, 64)
	if err != nil {
		return 0, nil
	}
	return i64, nil
}

func AllTypeToUInt64(s interface{}) (uint64, error) {
	t := strings.Trim(fmt.Sprint(s), " ")
	t = strings.Trim(t, "　")
	ui64, err := strconv.ParseUint(t, 10, 64)
	if err != nil {
		return 0, nil
	}
	return ui64, nil
}

func CkDataTypeTransfer(data interface{}, fieldName string, toDataType string,NullNotTransferDefault bool) (v interface{}, e error) {
	// 假如字段允许是 Nullable() ，允许为 null 的情况下，并设置的强制转成默认值，则直接写入 nil 值
	if NullNotTransferDefault == true && data == nil && toDataType[0:3] == "Nul" {
		return nil,nil
	}
	defer func() {
		if err := recover(); err != nil {
			e = fmt.Errorf(fieldName + " " + fmt.Sprint(err))
		}
	}()
	switch toDataType {
	case "Date", "Nullable(Date)":
		if data == nil {
			v = int16(0)
			break
		}
		switch data.(type) {
		case int16:
			v = data
			break
		case string:
			switch data.(string) {
			case "0000-00-00",""," ":
				v = int16(0)
				break
			default:
				v = data
				break
			}
			break
		default:
			i64, err := AllTypeToInt64(data)
			if err != nil {
				return 0, err
			}
			if i64 <= 32767 && i64 >= -32768 {
				v = int16(i64)
			} else {
				v = int16(0)
			}
			break
		}
		break
	case "DateTime", "Nullable(DateTime)":
		if data == nil {
			v = int32(0)
			break
		}
		switch data.(type) {
		case int32:
			v = data
			break
		case string:
			switch data.(string) {
			case "0000-00-00 00:00:00",""," ":
				v = int32(0)
				break
			default:
				loc, _ := time.LoadLocation("Local")                                          //重要：获取时区
				theTime, _ := time.ParseInLocation("2006-01-02 15:04:05", data.(string), loc) //使用模板在对应时区转化为time.time类型
				v = theTime.Unix()
				break
			}
			break
		default:
			i64, err := AllTypeToInt64(data)
			if err != nil {
				return 0, err
			}
			if i64 <= 2147483647 && i64 >= -2147483648 {
				v = int32(i64)
			} else {
				v = int32(0)
			}
			break
		}
		break
	case "String", "Enum8", "Enum16", "Enum", "UUID", "Nullable(String)", "Nullable(Enum8)", "Nullable(Enum16)", "Nullable(Enum)", "Nullable(UUID)":
		if data == nil {
			v = ""
			break
		}
		switch reflect.TypeOf(data).Kind() {
		case reflect.Array, reflect.Slice, reflect.Map:
			var c []byte
			c, e = json.Marshal(data)
			if e != nil {
				e = fmt.Errorf("field:%s ,data source type: %s , json.Marshal err: %s ", fieldName, reflect.TypeOf(data).Kind().String(), e.Error())
				return
			}
			v = string(c)
			break
		default:
			v = fmt.Sprint(data)
		}
		break
	case "Int8", "Nullable(Int8)":
		if data == nil {
			v = int8(0)
			break
		}
		switch data.(type) {
		case bool:
			if data.(bool) == true {
				v = int8(1)
			} else {
				v = int8(0)
			}
			break
		case int8:
			v = data
			break
		default:
			i64, err := AllTypeToInt64(data)
			if err != nil {
				return 0, err
			}
			if i64 <= 127 && i64 >= -128 {
				v = int8(i64)
			} else {
				v = int8(0)
			}
			break
		}
		break
	case "UInt8", "Nullable(UInt8)":
		if data == nil {
			v = uint8(0)
			break
		}
		switch data.(type) {
		case uint8:
			v = data
			break
		default:
			i64, err := AllTypeToUInt64(data)
			if err != nil {
				return 0, err
			}
			if i64 <= 255 {
				v = uint8(i64)
			} else {
				v = uint8(0)
			}
			break
		}
		break
	case "Int16", "Nullable(Int16)":
		if data == nil {
			v = int16(0)
			break
		}
		//mysql year 类型对应go int类型，但是ck里可能是Int16
		switch data.(type) {
		case int16:
			v = data
			break
		default:
			i64, err := AllTypeToInt64(data)
			if err != nil {
				return 0, err
			}
			if i64 <= 32767 && i64 >= -32768 {
				v = int16(i64)
			} else {
				v = int16(0)
			}
			break
		}
		break
	case "UInt16", "Nullable(UInt16)":
		if data == nil {
			v = uint16(0)
			break
		}
		switch data.(type) {
		case uint16:
			v = data
			break
		default:
			i64, err := AllTypeToUInt64(data)
			if err != nil {
				return 0, err
			}
			if i64 <= 65535 {
				v = uint16(i64)
			} else {
				v = uint16(0)
			}
			break
		}
		break
	case "Int32", "Nullable(Int32)":
		if data == nil {
			v = int32(0)
			break
		}
		switch data.(type) {
		case int32:
			v = data
			break
		default:
			i64, err := AllTypeToInt64(data)
			if err != nil {
				return 0, err
			}
			if i64 <= 2147483647 && i64 >= -2147483648 {
				v = int32(i64)
			} else {
				v = int32(0)
			}
			break
		}
		break
	case "UInt32", "Nullable(UInt32)":
		if data == nil {
			v = uint32(0)
			break
		}
		switch data.(type) {
		case uint32:
			v = data
			break
		default:
			i64, err := AllTypeToUInt64(data)
			if err != nil {
				return 0, err
			}
			if i64 <= 4294967295 {
				v = uint32(i64)
			} else {
				v = uint32(0)
			}
			break
		}
		break
	case "Int64", "Nullable(Int64)":
		if data == nil {
			v = int64(0)
			break
		}
		switch data.(type) {
		case int64:
			v = data
			break
		default:
			i64, err := AllTypeToInt64(data)
			if err != nil {
				return 0, err
			}
			v = i64
			break
		}
		break
	case "UInt64", "Nullable(UInt64)":
		if data == nil {
			v = uint64(0)
			break
		}
		switch data.(type) {
		case uint64:
			v = data
			break
		default:
			i64, err := AllTypeToUInt64(data)
			if err != nil {
				return 0, err
			}
			v = i64
			break
		}
		break
	case "Float64", "Nullable(Float64)":
		if data == nil {
			v = float64(0.00)
			break
		}
		// 有可能是decimal 类型，binlog解析出来decimal 对应go string类型
		switch data.(type) {
		case float64:
			v = data
			break
		case float32:
			v = float64(data.(float32))
			break
		default:
			v = interfaceToFloat64(data)
			break
		}
		break
	case "Float32", "Float", "Nullable(Float32)", "Nullable(Float)":
		if data == nil {
			v = float32(0.00)
			break
		}

		switch data.(type) {
		case float32:
			v = data
			break
		case float64:
			v = float32(data.(float64))
			break
		default:
			v = float32(interfaceToFloat64(data))
			break
		}
		break
	default:
		//Decimal
		if toDataType[0:3] == "Dec" || (toDataType[0:3] == "Nul" && strings.Contains(toDataType, "Decimal")) {
			v = interfaceToFloat64(data)
		} else {
			switch reflect.TypeOf(data).Kind() {
			case reflect.Array, reflect.Slice, reflect.Map:
				var c []byte
				c, e = json.Marshal(data)
				if e != nil {
					e = fmt.Errorf("field:%s ,data source type: %s , json.Marshal err: %s ", fieldName, reflect.TypeOf(data).Kind().String(), e.Error())
					return
				}
				v = string(c)
				break
			default:
				v = fmt.Sprint(data)
			}
		}
		break
	}
	return
}

func interfaceToFloat64(data interface{}) float64 {
	t := strings.Trim(fmt.Sprint(data), " ")
	t = strings.Trim(t, "　")
	f1, err := strconv.ParseFloat(t, 64)
	if err != nil {
		return float64(0.00)
	}
	return f1
}

func (This *Conn) TransferToCreateTableSql(data *pluginDriver.PluginDataType) (sql string, ckField []fieldStruct) {
	if data.Rows == nil || len(data.Rows) == 0 || len(data.Pri) == 0 {
		return "", nil
	}
	sql = "CREATE TABLE IF NOT EXISTS `" + This.GetSchemaName(data.SchemaName) + "`.`" + This.GetFieldName(data.TableName) + "` ("
	ckField = make([]fieldStruct, 0)
	var getToCkType = func(v interface{}) (toType string) {
		var err error
		if v != nil {
			switch reflect.TypeOf(v).Kind() {
			case reflect.Int8, reflect.Bool:
				toType = "Int8"
				break
			case reflect.Uint8:
				toType = "UInt8"
				break
			case reflect.Int16:
				toType = "Int16"
				break
			case reflect.Uint16:
				toType = "UInt16"
				break
			case reflect.Int32:
				toType = "Int32"
				break
			case reflect.Uint32:
				toType = "UInt32"
				break
			case reflect.Int, reflect.Int64:
				toType = "Int64"
				break
			case reflect.Uint, reflect.Uint64:
				toType = "UInt64"
				break
			case reflect.Float32:
				toType = "Float32"
				break
			case reflect.Float64:
				toType = "Float64"
				break
			case reflect.Map, reflect.Slice, reflect.Interface:
				toType = "String"
				break
			case reflect.String:
				switch len(v.(string))  {
				case 19:
					if v.(string) == "0000-00-00 00:00:00" {
						toType = "DateTime"
						break
					}
					_, err = time.Parse("2006-01-02 15:04:05", v.(string))
					if err == nil {
						toType = "DateTime"
					} else {
						toType = "String"
					}
					break
				case 10:
					if v.(string) == "0000-00-00" {
						toType = "Date"
						break
					}
					_, err = time.Parse("2006-01-02", v.(string))
					if err == nil {
						toType = "Date"
					} else {
						toType = "String"
					}
					break
				default:
					toType = "String"
					break
				}
				break
			default:
				toType = "String"
				break
			}
		} else {
			toType = "String"
		}
		return
	}
	var val = ""
	var addCkField = func(ckFieldName,mysqlFieldName,ckType string) {
		if val == "" {
			val = "`"+ckFieldName +"` " +ckType
		} else {
			val += ",`" + ckFieldName +"` "+ ckType
		}
		ckField = append(ckField, fieldStruct{CK: ckFieldName, MySQL: mysqlFieldName, CkType: ckType})
		return
	}
	priArr := make([]string, 0)
	priMap := make(map[string]bool,0)
	var toCkType string
	for _, priK := range data.Pri {
		fileName0 := This.GetFieldName(priK)
		priArr = append(priArr, fileName0)
		priMap[fileName0] = true
		toCkType = getToCkType(data.Rows[0][priK])
		addCkField(fileName0,priK,toCkType)
	}
	var ok bool
	for fileName, v := range data.Rows[0] {
		fileName0 := This.GetFieldName(fileName)
		if _,ok = priMap[fileName0];ok {
			continue
		}
		toCkType = getToCkType(v)
		toCkType = "Nullable("+toCkType+")"
		addCkField(fileName0,fileName,toCkType)
	}
	addCkField("bifrost_data_version","{$BifrostDataVersion}","Nullable(Int64)")
	addCkField("binlog_event_type","{$EventType}","Nullable(String)")
	sql += val + ") ENGINE = ReplacingMergeTree ORDER BY (" + strings.Join(priArr, ",") + ")"
	return
}

func (This *Conn) TransferToCreateDatabaseSql(SchemaName string) (sql string) {
	sql = "CREATE DATABASE IF NOT EXISTS `"+SchemaName+"`"
	return sql
}