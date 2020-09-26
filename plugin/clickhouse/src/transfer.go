package src

import (
	"strings"
	"time"
	"reflect"
	"fmt"
	"strconv"
	"encoding/json"
	"log"
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

func CkDataTypeTransfer(data interface{}, fieldName string, toDataType string) (v interface{}, e error) {
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
			if data.(string) == "0000-00-00" {
				v = int16(0)
			} else {
				v = data
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
			if data.(string) == "0000-00-00 00:00:00" {
				v = int32(0)
			} else {
				loc, _ := time.LoadLocation("Local")                                          //重要：获取时区
				theTime, _ := time.ParseInLocation("2006-01-02 15:04:05", data.(string), loc) //使用模板在对应时区转化为time.time类型
				v = theTime.Unix()
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

func TransferToCreateTableSql(SchemaName, TableName string, data map[string]interface{}, Pri []*string) (sql string, ckField []fieldStruct) {
	if data == nil || len(data) == 0 || len(Pri) == 0 {
		log.Println("data:",data)
		log.Println("pri:",Pri)
		return "", nil
	}
	sql = "CREATE TABLE IF NOT EXISTS `" + SchemaName + "`.`" + TableName + "` ("
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
		ckField = append(ckField, fieldStruct{CK: ckFieldName, MySQL: ckFieldName, CkType: ckType})
		return
	}
	priArr := make([]string, 0)
	priMap := make(map[string]bool,0)
	var toCkType string
	for _, priK := range Pri {
		priArr = append(priArr, *priK)
		priMap[*priK] = true
		toCkType = getToCkType(data[*priK])
		addCkField(*priK,*priK,toCkType)
	}
	var ok bool
	for fileName, v := range data {
		if _,ok = priMap[fileName];ok {
			continue
		}
		toCkType = getToCkType(v)
		addCkField(fileName,fileName,toCkType)
	}
	addCkField("bifrost_data_version","{$BifrostDataVersion}","String")
	addCkField("binlog_event_type","{$EventType}","String")
	sql += val + ") ENGINE = ReplacingMergeTree ORDER BY (" + strings.Join(priArr, ",") + ")"
	return
}

func TransferToCreateDatabaseSql(SchemaName string) (sql string) {
	sql = "CREATE DATABASE IF NOT EXISTS `"+SchemaName+"`"
	return sql
}