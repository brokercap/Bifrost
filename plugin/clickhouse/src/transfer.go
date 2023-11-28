package src

import (
	"encoding/json"
	"fmt"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
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

func CkDataTypeTransfer(data interface{}, fieldName string, toDataType string, NullNotTransferDefault bool) (v interface{}, e error) {
	// 假如字段允许是 Nullable() ，允许为 null 的情况下，并设置的强制转成默认值，则直接写入 nil 值
	if NullNotTransferDefault == true && data == nil && toDataType[0:3] == "Nul" {
		return nil, nil
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
			case "0000-00-00", "", " ":
				v = int16(0)
				break
			default:
				v = data
				break
			}
			break
		case float32:
			v = int16(data.(float32))
		case float64:
			v = int16(data.(float64))
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
		case float32:
			v = int32(data.(float32))
		case float64:
			v = int32(data.(float64))
		case string:
			switch data.(string) {
			case "0000-00-00 00:00:00", "", " ":
				v = int32(0)
				break
			default:
				if strings.Index(data.(string), "0000-00-00 00:00:00") == 0 {
					v = int64(0)
				} else {
					v = data
				}
				break
				/*
					loc, _ := time.LoadLocation("Local")                                          //重要：获取时区
					theTime, _ := time.ParseInLocation("2006-01-02 15:04:05", data.(string), loc) //使用模板在对应时区转化为time.time类型
					v = theTime.Unix()
					break
				*/
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
		case float32:
			v = int8(data.(float32))
		case float64:
			v = int8(data.(float64))
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
		case float32:
			v = uint8(data.(float32))
		case float64:
			v = uint8(data.(float64))
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
		case float32:
			v = int16(data.(float32))
		case float64:
			v = int16(data.(float64))
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
		case float32:
			v = uint16(data.(float32))
		case float64:
			v = uint16(data.(float64))
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
		case float32:
			v = int32(data.(float32))
		case float64:
			v = int32(data.(float64))
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
		case float32:
			v = uint32(data.(float32))
		case float64:
			v = uint32(data.(float64))
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
		case float32:
			v = int64(data.(float32))
		case float64:
			v = int64(data.(float64))
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
		case float32:
			v = uint64(data.(float32))
		case float64:
			v = uint64(data.(float64))
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
		//DateTime64
		if strings.Contains(toDataType, "DateTime64") {
			if data == nil {
				v = int64(0)
				break
			}
			switch data.(type) {
			case int32:
				v = int64(data.(int32))
				break
			case int64:
				v = data
				break
			case float32:
				v = int64(data.(float32))
			case float64:
				v = int64(data.(float64))
			case string:
				switch data.(string) {
				case "", " ":
					v = int64(0)
					break
				default:
					if strings.Index(data.(string), "0000-00-00 00:00:00") == 0 {
						v = int64(0)
					} else {
						v, _ = time.ParseInLocation("2006-01-02 15:04:05.999999", data.(string), time.Local)
					}
					break
				}
				break
			default:
				var err error
				v, err = AllTypeToInt64(data)
				if err != nil {
					return 0, err
				}
				break
			}
			break
		}
		//Decimal
		if strings.Contains(toDataType, "Decimal") {
			v = InterfaceToDecimalData(data, toDataType)
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
			case reflect.Float32:
				v = strconv.FormatFloat(float64(data.(float32)), 'E', -1, 32)
			case reflect.Float64:
				v = strconv.FormatFloat(data.(float64), 'E', -1, 64)
			default:
				v = fmt.Sprint(data)
			}
		}
		break
	}
	return
}

func interfaceToFloat64(data interface{}) float64 {
	switch data.(type) {
	case float32:
		return float64(data.(float32))
	case float64:
		return data.(float64)
	default:
		break
	}
	t := strings.Trim(fmt.Sprint(data), " ")
	t = strings.Trim(t, "　")
	f1, err := strconv.ParseFloat(t, 64)
	if err != nil {
		return float64(0.00)
	}
	return f1
}

func (This *Conn) TransferToCkTypeByColumnType(columnType string, nullable bool) (toType string) {
	toType = "String"
	switch columnType {
	case "uint64", "Nullable(uint64)":
		toType = "UInt64"
	case "int64", "Nullable(int64)":
		toType = "Int64"
	case "uint32", "Nullable(uint32)", "uint24", "Nullable(uint24)":
		toType = "UInt32"
	case "int32", "Nullable(int32)", "int24", "Nullable(int24)":
		toType = "Int32"
	case "uint16", "Nullable(uint16)":
		toType = "UInt16"
	case "int16", "Nullable(int16)", "year(4)", "Nullable(year(4))", "year(2)", "Nullable(year(2))":
		toType = "Int16"
	case "uint8", "Nullable(uint8)":
		toType = "UInt8"
	case "int8", "Nullable(int8)", "bool", "Nullable(bool)":
		toType = "Int8"
	case "float", "Nullable(float)":
		toType = "Float32"
	case "double", "Nullable(double)":
		toType = "Float64"
	case "date", "Nullable(date)":
		toType = "Date"
	default:
		if strings.Index(columnType, "double") >= 0 {
			toType = "Float64"
			break
		}
		if strings.Index(columnType, "float") >= 0 {
			toType = "Float32"
			break
		}
		if strings.Index(columnType, "bit") >= 0 {
			toType = "Int64"
			break
		}
		if strings.Index(columnType, "timestamp") >= 0 {
			i := strings.Index(columnType, "timestamp(")
			if i >= 0 {
				// 0000-00-00 00:00:00.000000
				// 由于 ck DateTime64 在19.19 某个小版本开始支持，考滤分支过细的问题，我们统一以20版本开始支持 DateTime64 转换
				if This.ckVersion >= 2000000000 || This.ckVersion == 0 {
					nsecNum := strings.Split(columnType[i+10:], ")")[0]
					toType = "DateTime64(" + nsecNum + ")"
				} else {
					toType = "String"
				}
				break
			}
			toType = "DateTime"
			break
		}
		if strings.Index(columnType, "datetime") >= 0 {
			i := strings.Index(columnType, "datetime(")
			if i >= 0 {
				if This.ckVersion >= 2000000000 || This.ckVersion == 0 {
					nsecNum := strings.Split(columnType[i+9:], ")")[0]
					toType = "DateTime64(" + nsecNum + ")"
				} else {
					toType = "String"
				}
				break
			}
			toType = "DateTime"
			break
		}
		if strings.Index(columnType, "decimal") >= 0 {
			i := strings.Index(columnType, "decimal(")
			if i < 0 {
				toType = "Decimal(18,2)"
				break
			}
			dataTypeParam := strings.Split(columnType[i+8:], ")")[0]
			dataTypeParam = strings.Trim(dataTypeParam, " ")
			if dataTypeParam == "" {
				toType = "Decimal(18,2)"
				break
			}
			p := strings.Split(dataTypeParam, ",")
			M, _ := strconv.Atoi(strings.Trim(p[0], " "))
			var D int
			if len(p) == 2 {
				D, _ = strconv.Atoi(strings.Trim(p[1], " "))
			}
			// M,D.   M > 18 就属于 Decimal128 , M > 39 就属于 Decimal256  ，但是当前你 go ck 驱动只支持 Decimal64
			if M > 18 {
				toType = "String"
			} else {
				toType = fmt.Sprintf("Decimal(%d,%d)", M, D)
			}
			break
		}
	}
	if nullable {
		if strings.Index(columnType, "Nullable") >= 0 {
			toType = "Nullable(" + toType + ")"
		}
	}
	return
}

func (This *Conn) TransferToCkTypeByColumnData(v interface{}, nullable bool) (toType string) {
	toType = "String"
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
		case reflect.Map, reflect.Slice, reflect.Interface, reflect.Array:
			toType = "String"
			break
		case reflect.String:
			switch v.(type) {
			case json.Number:
				goto outer
				break
			default:
				break
			}
			n := len(v.(string))
			switch n {
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
				// 0000-00-00 00:00:00.000000
				// 由于 ck DateTime64 在19.19 某个小版本开始支持，考滤分支过细的问题，我们统一以20版本开始支持 DateTime64 转换
				if This.ckVersion >= 2000000000 || This.ckVersion == 0 {
					if n > 19 && n <= 26 {
						nsec := fmt.Sprintf("%0*d", n-20, 0)
						_, err = time.Parse("2006-01-02 15:04:05."+nsec, v.(string))
						if err == nil {
							toType = "DateTime64(" + fmt.Sprint(n-20) + ")"
						}
					}
				}
				break
			}
			break
		default:
			break
		}
	}
outer:
	if nullable {
		toType = "Nullable(" + toType + ")"
	}
	return
}

func (This *Conn) TransferToCreateTableSql(data *pluginDriver.PluginDataType) (sql string, distributeSql, viewSql string, ckField []fieldStruct) {
	if data.Rows == nil || len(data.Rows) == 0 || len(data.Pri) == 0 {
		return "", "", "", nil
	}

	switch This.p.CkEngine {
	case 1: //单节点
		sql = "CREATE TABLE IF NOT EXISTS `" + This.GetSchemaName(data.SchemaName) + "`.`" + This.GetTableName(data.TableName) + "` ("
	case 2: //集群
		if This.p.CkClusterName == "" {
			return "", "", "", nil
		}
		schemaNameCase1 := "`" + This.GetSchemaName(data.SchemaName) + "`"
		tableNameLocalCase1 := "`" + This.GetTableName(data.TableName) + "_local`"
		tableNameDisCase1 := "`" + This.GetTableName(data.TableName) + "_all`"
		tableNameViewCase1 := "`" + This.GetTableName(data.TableName) + "_all_pview`"

		sql = "CREATE TABLE IF NOT EXISTS " + schemaNameCase1 + "." + tableNameLocalCase1 + " on cluster " + This.p.CkClusterName + " ("
		distributeSql = "CREATE TABLE IF NOT EXISTS " + schemaNameCase1 + "." + tableNameDisCase1 + "  on cluster " + This.p.CkClusterName + " ("
		viewSql = fmt.Sprintf("create view IF NOT EXISTS %s.%s on cluster %s as "+
			"select * from %s.%s final",
			schemaNameCase1, tableNameViewCase1, This.p.CkClusterName, schemaNameCase1, tableNameDisCase1)
	}

	ckField = make([]fieldStruct, 0)
	var getToCkType = func(fieldName string, nullable bool) string {
		if data.ColumnMapping != nil {
			if columnType, ok := data.ColumnMapping[fieldName]; ok {
				return This.TransferToCkTypeByColumnType(columnType, nullable)
			}
		}
		return This.TransferToCkTypeByColumnData(data.Rows[len(data.Rows)-1][fieldName], nullable)
	}
	var val = ""
	var addCkField = func(ckFieldName, mysqlFieldName, ckType string) {
		if val == "" {
			val = "`" + strings.Trim(ckFieldName, " ") + "` " + ckType
		} else {
			val += ",`" + strings.Trim(ckFieldName, " ") + "` " + ckType
		}
		ckField = append(ckField, fieldStruct{CK: ckFieldName, MySQL: mysqlFieldName, CkType: ckType})
		return
	}
	priArr := make([]string, 0)
	priMap := make(map[string]bool, 0)
	var toCkType string
	for _, priK := range data.Pri {
		fileName0 := This.GetFieldName(priK)
		priArr = append(priArr, fileName0)
		priMap[fileName0] = true
		toCkType = getToCkType(priK, false)
		addCkField(fileName0, priK, toCkType)
	}
	var ok bool
	for fileName, _ := range data.Rows[len(data.Rows)-1] {
		fileName0 := This.GetFieldName(fileName)
		if _, ok = priMap[fileName0]; ok {
			continue
		}
		toCkType = getToCkType(fileName, true)
		addCkField(fileName0, fileName, toCkType)
	}
	addCkField("binlog_timestamp", "{$BinlogTimestamp}", "Int64")
	addCkField("bifrost_data_version", "{$BifrostDataVersion}", "Int64")
	addCkField("binlog_event_type", "{$EventType}", "String")

	switch This.p.CkEngine {
	case 1: //单机
		engingName, partitionBy, orderBy := This.GetEngineAndOrderBy(priArr)
		if partitionBy != "" {
			partitionBy = fmt.Sprintf("PARTITION BY (%s)", partitionBy)
		}
		sql += val + fmt.Sprintf(") ENGINE = %s %s ORDER BY (%s)", engingName, partitionBy, orderBy)
	case 2: //集群
		engingName, partitionBy, orderBy := This.GetClusterEngineAndOrderBy(priArr)
		if partitionBy != "" {
			partitionBy = fmt.Sprintf("PARTITION BY (%s)", partitionBy)
		}
		sql += val + ") ENGINE = " + engingName + "('/bifrost/clickhouse/" + This.p.CkClusterName + "/tables/" + This.GetSchemaName(data.SchemaName) + "." + This.GetTableName(data.TableName) + "_local" + "/{shard}', '{replica}') " + partitionBy + " ORDER BY (" + orderBy + ")"
		distributeSql += val + ") ENGINE = Distributed(" + This.p.CkClusterName + ", " + This.GetSchemaName(data.SchemaName) + ", " + This.GetTableName(data.TableName) + "_local" + ",sipHash64(" + orderBy + "))"
	}
	return
}

func (This *Conn) GetEngineAndOrderBy(priArr []string) (engingName string, partitionBy string, orderBy string) {
	switch This.p.CkTableEngine {
	case "MergeTree":
		engingName = "MergeTree"
		partitionBy = "toYYYYMM(toDateTime(binlog_timestamp))"
		orderBy = "binlog_timestamp,binlog_event_type," + strings.Join(priArr, ",")
	default:
		engingName = "ReplacingMergeTree(bifrost_data_version)"
		orderBy = strings.Join(priArr, ",")
	}
	return
}

func (This *Conn) GetClusterEngineAndOrderBy(priArr []string) (engingName string, partitionBy string, orderBy string) {
	switch This.p.CkTableEngine {
	case "MergeTree":
		engingName = "ReplicatedMergeTree"
		partitionBy = "toYYYYMM(toDateTime(binlog_timestamp))"
		orderBy = "binlog_timestamp,binlog_event_type," + strings.Join(priArr, ",")
	default:
		engingName = "ReplicatedReplacingMergeTree"
		orderBy = strings.Join(priArr, ",")
	}
	return
}

func (This *Conn) TransferToCreateDatabaseSql(SchemaName string) (sql string) {
	switch This.p.CkEngine {
	case 1: //单节点
		sql = "CREATE DATABASE IF NOT EXISTS `" + SchemaName + "`"
	case 2: //集群
		if This.p.CkClusterName == "" {
			return
		}
		sql = "CREATE DATABASE IF NOT EXISTS `" + SchemaName + "` on cluster " + This.p.CkClusterName + ""
	}
	return sql
}

func ReplaceBr(str string) string {
	str = strings.ReplaceAll(str, "\r\n", " ")
	str = strings.ReplaceAll(str, "\n", " ")
	str = strings.ReplaceAll(str, "\r", " ")
	return str
}

// 去除连续的两个空格
func ReplaceTwoReplace(sql string) string {
	for {
		if strings.Index(sql, "  ") >= 0 {
			sql = strings.ReplaceAll(sql, "  ", " ")
			//sql = strings.ReplaceAll(sql,"	"," ")    // 这两个是不一样的，一个是两个 " "+" "，一个是" "+""
		} else {
			break
		}
	}
	for {
		if strings.Index(sql, "	") >= 0 {
			sql = strings.ReplaceAll(sql, "	", " ") // 这两个是不一样的，一个是两个 " "+" "，一个是" "+""
		} else {
			return sql
		}
	}
}

// 将sql 里 /* */ 注释内容给去掉
// 感谢 @zeroone2005 正则表达式提供支持
var replaceSqlNotesReq = regexp.MustCompile(`/\*(.*?)\*/`)

func TransferNotes2Space(sql string) string {
	sql = replaceSqlNotesReq.ReplaceAllString(sql, "")
	return sql
}
