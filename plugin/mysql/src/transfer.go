package src

import (
	"encoding/json"
	"errors"
	"fmt"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"log"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func (This *Conn) GetToDestDataType(data *pluginDriver.PluginDataType, fieldName string, nullable bool) (dataType string) {
	if data.ColumnMapping != nil {
		if columnType, ok := data.ColumnMapping[fieldName]; ok {
			return This.TransferToTypeByColumnType_Starrocks(columnType, nullable)
		}
	}
	return This.TransferToCkTypeByColumnData(data.Rows[len(data.Rows)-1][fieldName], nullable)
}

func (This *Conn) TransferToCreateTableSql(data *pluginDriver.PluginDataType) (sql string) {
	if !This.IsStarRocks() {
		log.Printf("[ERROR] output[%s] only starrocks server support auto create table \n", OutputName)
		return ""
	}
	if data.Rows == nil || len(data.Rows) == 0 {
		return ""
	}
	if len(data.Pri) == 0 && This.p.SyncMode != SYNCMODE_LOG_APPEND {
		log.Printf("[ERROR] output[%s] only SyncMode:%s support no pri,bug current SyncMode:%s SchemaName:%s TableName:%s \n", OutputName, SYNCMODE_LOG_APPEND, This.p.SyncMode, data.SchemaName, data.TableName)
		return ""
	}
	var fieldsStr string
	var isFirst = true
	var addCkField = func(destFieldName, mysqlFieldName, ckType string) {
		if isFirst {
			fieldsStr += fmt.Sprintf("`%s` %s", strings.Trim(destFieldName, " "), ckType)
			isFirst = false
		} else {
			fieldsStr += fmt.Sprintf(",`%s` %s", strings.Trim(destFieldName, " "), ckType)
		}
		return
	}
	if This.p.SyncMode == SYNCMODE_LOG_APPEND {
		// starrocks append 模式是采用 binlog_datetime,binlog_event_type,$pks 作为进行排序,建表的时候,字段必须是在前面才能建成功,下同
		addCkField("binlog_datetime", "{$BinlogDateTime}", "DATETIME DEFAULT NULL")
		addCkField("binlog_event_type", "{$EventType}", "CHAR(6) DEFAULT NULL")
	}
	priMap := make(map[string]bool, 0)
	for _, fileName0 := range data.Pri {
		priMap[fileName0] = true
		toDataType := This.GetToDestDataType(data, fileName0, false)
		addCkField(fileName0, fileName0, toDataType)
	}

	var ok bool
	for fileName0, _ := range data.Rows[len(data.Rows)-1] {
		if _, ok = priMap[fileName0]; ok {
			continue
		}
		toDataType := This.GetToDestDataType(data, fileName0, true)
		addCkField(fileName0, fileName0, toDataType)
	}
	if This.p.SyncMode != SYNCMODE_LOG_APPEND {
		// starrocks 由于普通模式,是采用源端主键作为主键的,所以这些字段 放到表最后面就行
		addCkField("binlog_datetime", "{$BinlogDateTime}", "DATETIME DEFAULT NULL")
		addCkField("binlog_event_type", "{$EventType}", "CHAR(6) DEFAULT NULL")
	}
	engineSQL, err := This.GetCreateTableEngine(data)
	if err != nil {
		log.Printf("[ERROR] output[%s] TransferToCreateTableSql err:%+v \n", OutputName, err)
		return ""
	}
	sql = fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` (%s) %s", This.GetSchemaName(data), This.GetTableName(data), fieldsStr, engineSQL)
	return
}

func (This *Conn) GetCreateTableEngine(data *pluginDriver.PluginDataType) (engineSQL string, err error) {
	if This.IsStarRocks() {
		return This.GetCreateTableEngineByStarRocks(data)
	}
	return This.GetCreateTableEngineByMysql(data)
}

func (This *Conn) GetCreateTableEngineByMysql(data *pluginDriver.PluginDataType) (engineSQL string, err error) {
	err = errors.New("mysql not supported auto create table")
	return
}

func (This *Conn) GetCreateTableEngineByStarRocks(data *pluginDriver.PluginDataType) (engineSQL string, err error) {
	engineSQL = " ENGINE=OLAP "
	var ids string
	if This.p.SyncMode != SYNCMODE_LOG_APPEND && len(data.Pri) == 0 {
		err = errors.New("no pri ,not supported")
		return
	}
	if len(data.Pri) > 0 {
		ids = strings.Replace(strings.Trim(fmt.Sprint(data.Pri), "[]"), " ", "','", -1)
	}
	if This.p.SyncMode == SYNCMODE_LOG_APPEND {
		if ids == "" {
			ids = "binlog_datetime,binlog_event_type," + ids
		} else {
			ids = "binlog_datetime,binlog_event_type"
		}
		engineSQL = fmt.Sprintf(" DUPLICATE KEY(%s) PARTITION BY date_trunc('month', binlog_datetime) DISTRIBUTED BY HASH(%s)", ids, ids)
	} else {
		engineSQL = fmt.Sprintf(" UNIQUE KEY(%s) DISTRIBUTED BY HASH(%s)", ids, ids)
	}
	if This.GetStarRocksBeCount() < 3 {
		engineSQL += fmt.Sprintf(" PROPERTIES ('replication_num' = '%d' )", 1)
	}
	return
}

// 在自动建表的情况下,并且是追加模式的时候 ,需要自动添加一个自增ID的主键
func (This *Conn) GetCreateAutoIncreFields() (ids []string) {
	if !This.p.AutoTable {
		return
	}
	// 必须是追加数据模式,才能自动添加一个自增ID主键
	if This.p.SyncMode != SYNCMODE_LOG_APPEND {
		return
	}
	return
	//return []string{BifrostAutoInrcFieldName}
}

func (This *Conn) TransferToTypeByColumnType(columnType string, nullable bool) (toType string) {
	if This.IsStarRocks() {
		return This.TransferToTypeByColumnType_Starrocks(columnType, nullable)
	}
	return "TEXT"
}

func (This *Conn) TransferToTypeByColumnType_Starrocks(columnType string, nullable bool) (toType string) {
	toType = "STRING"
	// starrocks 测试下来当前是不支持 无符号数字,所以需要给相对应的无符号数字加大一个等级的空间
	// uint64 则需要使用STRING
	switch columnType {
	case "uint64", "Nullable(uint64)":
		toType = "STRING"
	case "int64", "Nullable(int64)":
		toType = "BIGINT(20)"
	case "uint32", "Nullable(uint32)", "uint24", "Nullable(uint24)":
		toType = "BIGINT(20)"
	case "int32", "Nullable(int32)", "int24", "Nullable(int24)":
		toType = "INT(11)"
	case "uint16", "Nullable(uint16)":
		toType = "INT(11)"
	case "int16", "Nullable(int16)", "year(4)", "Nullable(year(4))", "year(2)", "Nullable(year(2))":
		toType = "SMALLINT(6)"
	case "uint8", "Nullable(uint8)":
		toType = "SMALLINT(6)"
	case "int8", "Nullable(int8)", "bool", "Nullable(bool)":
		toType = "TINYINT(4)"
	case "float", "Nullable(float)":
		toType = "FLOAT"
	case "double", "Nullable(double)":
		toType = "DOUBLE"
	case "decimal", "Nullable(decimal)":
		toType = "DECIMAL"
	case "date", "Nullable(date)":
		toType = "DATE"
	case "json":
		toType = "JSON"
	default:
		if strings.Index(columnType, "double") >= 0 {
			toType = "DOUBLE"
			break
		}
		if strings.Index(columnType, "float") >= 0 {
			toType = "FLOAT"
			break
		}
		if strings.Index(columnType, "bit") >= 0 {
			toType = "BIGINT(20)"
			break
		}
		if strings.Index(columnType, "timestamp") >= 0 {
			toType = "DATETIME"
			break
		}
		if strings.Index(columnType, "datetime") >= 0 {
			toType = "DATETIME"
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
				toType = "STRING"
			} else {
				toType = fmt.Sprintf("Decimal(%d,%d)", M, D)
			}
			break
		}
	}
	if nullable {
		if strings.Index(columnType, "Nullable") >= 0 {
			toType += " DEFAULT NULL"
		}
	}
	return
}

func (This *Conn) TransferToCkTypeByColumnData(v interface{}, nullable bool) (toType string) {
	toType = "STRING"
	var err error
	if v != nil {
		switch reflect.TypeOf(v).Kind() {
		case reflect.Int8, reflect.Bool:
			toType = "TINYINT(4)"
			break
		case reflect.Uint8:
			toType = "SMALLINT(6)"
			break
		case reflect.Int16:
			toType = "SMALLINT(6)"
			break
		case reflect.Uint16:
			toType = "INT(11)"
			break
		case reflect.Int32:
			toType = "INT(11)"
			break
		case reflect.Uint32:
			toType = "BIGINT(20)"
			break
		case reflect.Int, reflect.Int64:
			toType = "BIGINT(20)"
			break
		case reflect.Uint, reflect.Uint64:
			toType = "STRING"
			break
		case reflect.Float32:
			toType = "FLOAT"
			break
		case reflect.Float64:
			toType = "DOUBLE"
			break
		case reflect.Map, reflect.Slice, reflect.Array:
			toType = "JSON"
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
					toType = "DATETIME"
				}
				break
			case 10:
				if v.(string) == "0000-00-00" {
					toType = "DATE"
					break
				}
				_, err = time.Parse("2006-01-02", v.(string))
				if err == nil {
					toType = "DATE"
				}
				break
			default:
				if n > 19 && n <= 26 {
					nsec := fmt.Sprintf("%0*d", n-20, 0)
					_, err = time.Parse("2006-01-02 15:04:05."+nsec, v.(string))
					if err == nil {
						toType = "DATETIME(" + fmt.Sprint(n-20) + ")"
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
	if !nullable {
		toType += toType + " NOT NULL"
	}
	return
}
