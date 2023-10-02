package main

import (
	"database/sql/driver"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/brokercap/Bifrost/Bristol/mysql"
	"log"
	"math/rand"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const VERSION = "2.1.1"

var errDataList []string

var defaultValBool bool

var MysqlVersion string

func init() {
	errDataList = make([]string, 0)
	rand.Seed(time.Now().UnixNano())
	if rand.Intn(3) == 1 {
		defaultValBool = false
	}
}

func proccessExit() {
	for _, logInfo := range errDataList {
		log.Println(logInfo)
	}
	fmt.Println("mysql server:", MysqlVersion)
}

func DBConnect(uri string) mysql.MysqlConnection {
	db := mysql.NewConnect(uri)
	return db
}

type MasterBinlogInfoStruct struct {
	File              string
	Position          int
	Binlog_Do_DB      string
	Binlog_Ignore_DB  string
	Executed_Gtid_Set string
}

func GetBinLogInfo(db mysql.MysqlConnection) MasterBinlogInfoStruct {
	sql := "SHOW MASTER STATUS"
	stmt, err := db.Prepare(sql)
	if err != nil {
		log.Println(err)
		return MasterBinlogInfoStruct{}
	}
	defer stmt.Close()
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	defer rows.Close()
	if err != nil {
		log.Printf("%v\n", err)
		return MasterBinlogInfoStruct{}
	}
	var File string
	var Position int
	var Binlog_Do_DB string
	var Binlog_Ignore_DB string
	var Executed_Gtid_Set string
	for {
		dest := make([]driver.Value, 5, 5)
		errs := rows.Next(dest)
		if errs != nil {
			return MasterBinlogInfoStruct{}
		}
		File = dest[0].(string)
		Binlog_Do_DB = dest[2].(string)
		Binlog_Ignore_DB = dest[3].(string)
		if dest[4] != nil {
			Executed_Gtid_Set = dest[4].(string)
		}
		PositonString := fmt.Sprint(dest[1])
		Position, _ = strconv.Atoi(PositonString)
		break
	}

	return MasterBinlogInfoStruct{
		File:              File,
		Position:          Position,
		Binlog_Do_DB:      Binlog_Do_DB,
		Binlog_Ignore_DB:  Binlog_Ignore_DB,
		Executed_Gtid_Set: Executed_Gtid_Set,
	}
}

func GetServerId(db mysql.MysqlConnection) int {
	sql := "show variables like 'server_id'"
	stmt, err := db.Prepare(sql)
	if err != nil {
		log.Println(err)
		return 0
	}
	defer stmt.Close()
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	if err != nil {
		log.Printf("%v\n", err)
		return 0
	}
	defer rows.Close()
	var ServerId int
	for {
		dest := make([]driver.Value, 2, 2)
		errs := rows.Next(dest)
		if errs != nil {
			return 0
		}
		ServerIdString := fmt.Sprint(dest[1])
		ServerId, _ = strconv.Atoi(ServerIdString)
		break
	}
	return ServerId
}

func GetVariables(db mysql.MysqlConnection, variablesValue string) (data map[string]string) {
	data = make(map[string]string, 0)
	sql := "show variables like '" + variablesValue + "'"
	stmt, err := db.Prepare(sql)
	if err != nil {
		log.Println(err)
		return
	}
	defer stmt.Close()
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}
	defer rows.Close()
	for {
		dest := make([]driver.Value, 2, 2)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		variableName := dest[0].(string)
		value := dest[1].(string)
		data[variableName] = value
	}
	return
}

func GetMySQLVersion(db mysql.MysqlConnection) string {
	sql := "SELECT version()"
	stmt, err := db.Prepare(sql)
	if err != nil {
		log.Println(err)
		return ""
	}
	defer stmt.Close()
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	if err != nil {
		log.Printf("sql:%s, err:%v\n", sql, err)
		return ""
	}
	defer rows.Close()
	var version string
	for {
		dest := make([]driver.Value, 1, 1)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		version = dest[0].(string)
		break
	}
	return version
}

func ExecSQL(db mysql.MysqlConnection, sql string) {
	p := make([]driver.Value, 0)
	_, err := db.Exec(sql, p)
	if err != nil {
		log.Println("sql:", sql)
		log.Println("err: ", err)
	}
	return
}

func evenTypeName(e mysql.EventType) string {
	switch e {
	case mysql.WRITE_ROWS_EVENTv0, mysql.WRITE_ROWS_EVENTv1, mysql.WRITE_ROWS_EVENTv2:
		return "insert"
	case mysql.UPDATE_ROWS_EVENTv0, mysql.UPDATE_ROWS_EVENTv1, mysql.UPDATE_ROWS_EVENTv2:
		return "update"
	case mysql.DELETE_ROWS_EVENTv0, mysql.DELETE_ROWS_EVENTv1, mysql.DELETE_ROWS_EVENTv2:
		return "delete"
	}
	return fmt.Sprintf("%d", e)
}

type Column struct {
	ColumnName             string
	ColumnKey              string
	ColumnDefault          string
	DataType               string
	Extra                  string
	ColumnType             string
	CharacterSetName       string
	CollationName          string
	NumbericScale          int
	IsBool                 bool
	Unsigned               bool
	IsPrimary              bool
	AutoIncrement          bool
	EnumValues             []string
	SetValues              []string
	CharacterMaximumLength int
	NumbericPrecision      int
	Fsp                    int
	IsNullable             string
	Value                  interface{}
}

func GetRandomString(l int, cn int) string {
	str := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ^&*'\";\\/%$#@90-_|<>?{}[]+.!~`,=0"
	str2Arr := []string{"测", "试", "数", "据"}
	bytes := []byte(str)
	result1 := []byte{}
	result2 := ""
	for i := 0; i < l; i++ {
		rand.Seed(time.Now().UnixNano() + int64(i))
		result1 = append(result1, bytes[rand.Intn(len(bytes))])
	}
	for i := 0; i < cn; i++ {
		rand.Seed(time.Now().UnixNano() + int64(i))
		result2 += str2Arr[rand.Intn(len(str2Arr))]
	}
	rand.Seed(time.Now().UnixNano())
	return string(result1) + result2
}

func GetTimeAndNsen(dataType string, fsp int) string {
	var timeFormat string
	switch dataType {
	case "time":
		if defaultValBool {
			if fsp > 0 {
				return "00:00:00." + fmt.Sprintf("%0*d", fsp, 0)
			} else {
				return "00:00:00"
			}
		}
		timeFormat = "15:03:04"
	case "timestamp", "datetime":
		if defaultValBool {
			if fsp > 0 {
				return "0000-00-00 00:00:00." + fmt.Sprintf("%0*d", fsp, 0)
			} else {
				return "0000-00-00 00:00:00"
			}
		}
		timeFormat = "2006-01-02 15:03:04"
	case "year":
		timeFormat = "2006"
	default:
		return ""
	}
	if fsp > 0 {
		timeFormat += "." + fmt.Sprintf("%0*d", fsp, 0)
	}
	value := time.Now().Format(timeFormat)
	i := strings.Index(value, ".")
	if i > 0 {
		rand.Seed(time.Now().UnixNano())
		if rand.Intn(2) >= 1 {
			value = strings.Replace(value, value[i:i+2], ".0", 1)
			value = value[0:len(value)-1] + "0"
		}
	}
	return value
}

func GetSchemaTableFieldAndVal(db mysql.MysqlConnection, schema string, table string) (autoIncrementField string, sqlstring string, data []driver.Value, columnData map[string]*Column, ColumnList []Column) {
	sql := "SELECT COLUMN_NAME,COLUMN_KEY,COLUMN_TYPE,CHARACTER_SET_NAME,COLLATION_NAME,NUMERIC_SCALE,EXTRA,COLUMN_DEFAULT,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION,IS_NULLABLE FROM `information_schema`.`COLUMNS` WHERE TABLE_SCHEMA = '" + schema + "' AND  table_name = '" + table + "'"
	data = make([]driver.Value, 0)
	stmt, err := db.Prepare(sql)
	columnList := make([]Column, 0)
	if err != nil {
		log.Println(err)
		return "", "", make([]driver.Value, 0), columnData, columnList
	}
	p := make([]driver.Value, 0)
	//p = append(p,schema)
	//p = append(p,table)
	rows, err := stmt.Query(p)
	defer rows.Close()
	if err != nil {
		log.Printf("%v\n", err)
		return "", "", make([]driver.Value, 0), columnData, columnList
	}
	columnData = make(map[string]*Column, 0)
	var sqlk, sqlv_, sqlval = "", "", ""
	for {
		dest := make([]driver.Value, 12, 12)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		var COLUMN_NAME, COLUMN_KEY, COLUMN_TYPE string
		var CHARACTER_SET_NAME, COLLATION_NAME, EXTRA string
		var NUMERIC_SCALE int
		var isBool bool = false
		var unsigned bool = false
		var is_primary bool = false
		var auto_increment bool = false
		var enum_values, set_values []string
		var COLUMN_DEFAULT string
		var DATA_TYPE string
		var CHARACTER_MAXIMUM_LENGTH int
		var NUMERIC_PRECISION int
		var IS_NULLABLE string

		COLUMN_NAME = fmt.Sprint(dest[0])
		COLUMN_KEY = fmt.Sprint(dest[1])
		COLUMN_TYPE = fmt.Sprint(dest[2])
		if dest[3] == nil {
			CHARACTER_SET_NAME = "NULL"
		} else {
			CHARACTER_SET_NAME = fmt.Sprint(dest[3])
		}

		if dest[4] == nil {
			COLLATION_NAME = "NULL"
		} else {
			COLLATION_NAME = fmt.Sprint(dest[4])
		}

		if dest[5] == nil {
			NUMERIC_SCALE = int(0)
		} else {
			NUMERIC_SCALE, _ = strconv.Atoi(fmt.Sprint(dest[5]))
		}

		EXTRA = fmt.Sprint(dest[6])

		DATA_TYPE = fmt.Sprint(dest[8])

		//bit类型这个地方比较特殊，不能直接转成string，并且当前只有 time,datetime 类型转换的时候会用到 默认值，这里不进行其他细节处理
		if DATA_TYPE != "bit" {
			if dest[7] == nil {
				COLUMN_DEFAULT = "NULL"
			} else {
				COLUMN_DEFAULT = fmt.Sprint(dest[7])
			}
		}

		if COLUMN_TYPE == "tinyint(1)" {
			isBool = true
		}
		if EXTRA == "auto_increment" {
			autoIncrementField = COLUMN_NAME
			auto_increment = true
		}
		if strings.Contains(COLUMN_TYPE, "unsigned") {
			unsigned = true
		}
		if COLUMN_KEY != "" {
			is_primary = true
		}

		if DATA_TYPE == "enum" {
			d := strings.Replace(COLUMN_TYPE, "enum(", "", -1)
			d = strings.Replace(d, ")", "", -1)
			d = strings.Replace(d, "'", "", -1)
			enum_values = strings.Split(d, ",")
		} else {
			enum_values = make([]string, 0)
		}

		if DATA_TYPE == "set" {
			d := strings.Replace(COLUMN_TYPE, "set(", "", -1)
			d = strings.Replace(d, ")", "", -1)
			d = strings.Replace(d, "'", "", -1)
			set_values = strings.Split(d, ",")
		} else {
			set_values = make([]string, 0)
		}

		if dest[9] == nil {
			CHARACTER_MAXIMUM_LENGTH = int(0)
		} else {
			CHARACTER_MAXIMUM_LENGTH, _ = strconv.Atoi(fmt.Sprint(dest[9]))
		}

		if dest[10] == nil {
			NUMERIC_PRECISION = int(0)
		} else {
			NUMERIC_PRECISION, _ = strconv.Atoi(fmt.Sprint(dest[10]))
		}

		var fsp int
		switch strings.ToLower(DATA_TYPE) {
		case "timestamp", "datetime", "time":
			columnDataType := strings.ToLower(COLUMN_TYPE)
			i := strings.Index(columnDataType, "(")
			if i <= 0 {
				break
			}
			fsp, _ = strconv.Atoi(columnDataType[i+1 : len(columnDataType)-1])
			break
		default:
			break
		}
		if dest[11] == nil {
			IS_NULLABLE = "NO"
		} else {
			IS_NULLABLE = dest[11].(string)
		}

		columnType := &Column{
			ColumnName:             COLUMN_NAME,
			ColumnKey:              COLUMN_KEY,
			ColumnType:             COLUMN_TYPE,
			ColumnDefault:          COLUMN_DEFAULT,
			DataType:               DATA_TYPE,
			Extra:                  EXTRA,
			CharacterSetName:       CHARACTER_SET_NAME,
			CollationName:          COLLATION_NAME,
			NumbericScale:          NUMERIC_SCALE,
			IsBool:                 isBool,
			Unsigned:               unsigned,
			IsPrimary:              is_primary,
			AutoIncrement:          auto_increment,
			EnumValues:             enum_values,
			SetValues:              set_values,
			CharacterMaximumLength: CHARACTER_MAXIMUM_LENGTH,
			NumbericPrecision:      NUMERIC_PRECISION,
			Fsp:                    fsp,
			IsNullable:             IS_NULLABLE,
		}
		columnData[COLUMN_NAME] = columnType
		columnList = append(columnList, *columnType)

		rand.Seed(time.Now().UnixNano())
		var randResult int
		if rand.Intn(2) >= 1 {
			randResult = 1
		} else {
			randResult = 0
		}
		if EXTRA == "auto_increment" {
			continue
		}
		if IS_NULLABLE == "NO" {
			switch columnType.DataType {
			case "int", "tinyint", "smallint", "mediumint", "bigint":
				if columnType.IsBool {
					if randResult == 1 {
						data = append(data, "1")
						columnType.Value = true
					} else {
						data = append(data, "0")
						columnType.Value = false
					}
				} else {
					b := ""
					var Value interface{}
					switch columnType.DataType {
					case "tinyint":
						if columnType.Unsigned == true {
							Value = uint8(255)
						} else {
							if randResult == 1 {
								Value = int8(127)
							} else {
								Value = int8(-128)
							}
						}
						break
					case "smallint":
						if columnType.Unsigned == true {
							Value = uint16(65535)
						} else {
							if randResult == 1 {
								Value = int16(32767)
							} else {
								Value = int16(-32768)
							}
						}
						break
					case "mediumint":
						if columnType.Unsigned == true {
							Value = uint32(16777215)
						} else {
							if randResult == 1 {
								Value = int32(8388607)
							} else {
								Value = int32(-8388608)
							}
						}
						break
					case "int":
						if columnType.Unsigned == true {
							Value = uint32(4294967295)
						} else {
							if randResult == 1 {
								Value = int32(2147483647)
							} else {
								Value = int32(-2147483648)
							}
						}
						break
					case "bigint":
						if columnType.Unsigned == true {
							Value = 1844674407370955161
						} else {
							if randResult == 1 {
								Value = 9223372036854775807
							} else {
								Value = -9223372036854775808
							}
						}
						break
					}
					b = fmt.Sprint(Value)
					columnType.Value = Value
					data = append(data, b)
				}
				break
			case "char", "varchar":
				var enSize, cnSize int = 0, 0
				if strings.Contains(columnType.CharacterSetName, "utf") {
					if columnType.CharacterMaximumLength/4 > 0 {
						cnSize = rand.Intn(columnType.CharacterMaximumLength / 4)
						enSize = columnType.CharacterMaximumLength - cnSize*4
					} else {
						enSize = rand.Intn(columnType.CharacterMaximumLength - 1)
					}
				} else {
					enSize = rand.Intn(columnType.CharacterMaximumLength - 1)
				}
				Value := GetRandomString(enSize, cnSize)
				columnType.Value = Value
				data = append(data, Value)
				break
			case "tinytext", "tinyblob", "text", "mediumtext", "smalltext", "blob", "mediumblob", "smallblob", "longblob":
				var enSize, cnSize int = 0, 0
				rand.Seed(time.Now().UnixNano())

				var n int
				if longstring == "true" {
					if columnType.ColumnType == "longblob" {
						//n = rand.Intn(65535/4)
						n = 163841
					} else {
						n = rand.Intn(columnType.CharacterMaximumLength / 4)
					}
				} else {
					n = rand.Intn(255 / 4)
				}

				if n == 0 {
					n = 1
				}
				if strings.Contains(columnType.CharacterSetName, "utf") {
					cnSize = rand.Intn(n)
				}
				enSize = n - cnSize
				Value := GetRandomString(enSize, cnSize)
				columnType.Value = Value
				data = append(data, Value)
				break
			case "year":
				Value := time.Now().Format("2006")
				columnType.Value = Value
				data = append(data, Value)
				break
			case "time":
				Value := GetTimeAndNsen(columnType.DataType, columnType.Fsp)
				columnType.Value = Value
				data = append(data, Value)
				break
			case "date":
				Value := time.Now().Format("2006-01-02")
				columnType.Value = Value
				data = append(data, Value)
				break
			case "datetime", "timestamp":
				/*
					假如 time.Time 类型提交到驱动,测试下来,在 8.0.34 的情况下存在 1秒的误差,这里强制使用 string 的方式提交
					if fsp == 0 {
						nowTime := time.Now()
						columnType.Value = nowTime.Format("2006-01-02 15:04:05")
						data = append(data, nowTime)
					} else {
						Value := GetTimeAndNsen(columnType.DataType, columnType.Fsp)
						columnType.Value = Value
						data = append(data, Value)
					}
				*/
				Value := GetTimeAndNsen(columnType.DataType, columnType.Fsp)
				columnType.Value = Value
				data = append(data, Value)

				break
			case "bit":
				var Value int64 = 1
				if columnType.NumbericPrecision < 16 {
					Value = int64(rand.Intn(127))
				}
				if columnType.NumbericPrecision >= 16 && columnType.NumbericPrecision < 32 {
					Value = int64(rand.Intn(32767))
				}
				if columnType.NumbericPrecision >= 32 && columnType.NumbericPrecision < 64 {
					Value = int64(rand.Int31())
				}
				if columnType.NumbericPrecision == 64 {
					Value = rand.Int63()
				}
				columnType.Value = Value
				data = append(data, Value)
				break
			case "float":
				Value := strconv.FormatFloat(float64(rand.Float32()), 'f', 2, 32)
				Value2, _ := strconv.ParseFloat(Value, 32)
				f1 := float32(rand.Intn(999999))
				f2 := f1 + float32(Value2)
				if randResult == 1 {
					f2 = 0 - f2
				}
				columnType.Value = f2
				data = append(data, fmt.Sprint(f2))
				break
			case "double":
				Value := strconv.FormatFloat(float64(rand.Float64()), 'f', 2, 64)
				Value2, _ := strconv.ParseFloat(Value, 64)
				f1 := float64(rand.Intn(999999))
				f2 := f1 + float64(Value2)
				if randResult == 1 {
					f2 = 0 - f2
				}
				columnType.Value = f2
				data = append(data, fmt.Sprint(f2))
				break
			case "decimal":
				Value := strconv.FormatFloat(float64(rand.Float64()), 'f', columnType.NumbericScale, 64)
				var n int = 1
				for i := 0; i < columnType.NumbericPrecision-columnType.NumbericScale; i++ {
					n *= 10
				}
				f1 := rand.Intn(n)
				if randResult == 1 {
					f1 = 0 - f1
				}
				index := strings.Index(Value, ".")
				value2 := fmt.Sprint(f1) + "." + Value[index+1:]
				columnType.Value = value2
				data = append(data, value2)
				break
			case "set":
				d := strings.Replace(COLUMN_TYPE, "set(", "", -1)
				d = strings.Replace(d, ")", "", -1)
				d = strings.Replace(d, "'", "", -1)
				set_values := strings.Split(d, ",")
				Value := make([]string, 0)
				//Value := set_values[rand.Intn(len(set_values)-1)]
				if len(set_values) > 1 {
					Value = append(Value, set_values[0])
					Value = append(Value, set_values[len(set_values)-1])
				} else {
					Value = append(Value, set_values[0])
				}
				columnType.Value = Value
				data = append(data, strings.Replace(strings.Trim(fmt.Sprint(Value), "[]"), " ", ",", -1))
				break
			case "enum":
				d := strings.Replace(COLUMN_TYPE, "enum(", "", -1)
				d = strings.Replace(d, ")", "", -1)
				d = strings.Replace(d, "'", "", -1)
				enum_values := strings.Split(d, ",")
				Value := enum_values[rand.Intn(len(enum_values)-1)]
				columnType.Value = Value
				data = append(data, Value)
				break
			case "json":
				m := make(map[string][]interface{}, 1)
				m2 := make(map[string]interface{}, 0)
				m3 := make(map[string]interface{}, 0)
				m2["key2"] = GetRandomString(20, 16)
				m3["key2"] = false
				m["key1"] = make([]interface{}, 0)
				m["key1"] = append(m["key1"], 2147483647)
				m["key1"] = append(m["key1"], -2147483648)
				m["key1"] = append(m["key1"], "2")
				m["key1"] = append(m["key1"], nil)
				m["key1"] = append(m["key1"], true)
				m["key1"] = append(m["key1"], 922337203685477)
				m["key1"] = append(m["key1"], -922337203685477)
				m["key1"] = append(m["key1"], m2)
				m["key1"] = append(m["key1"], m3)
				c, _ := json.Marshal(m)
				columnType.Value = m
				data = append(data, string(c))
				break
			default:
				data = append(data, "0")
				break
			}
		} else {
			columnType.Value = nil
			data = append(data, nil)
		}

		valTmp := data[len(data)-1]
		var sqlValTmp string
		switch valTmp.(type) {
		case nil:
			sqlValTmp = "null"
		case float32:
			sqlValTmp = strconv.FormatFloat(float64(valTmp.(float32)), 'E', -1, 32)
		case float64:
			sqlValTmp = strconv.FormatFloat(valTmp.(float64), 'E', -1, 64)
		default:
			sqlValTmp = strings.Replace(fmt.Sprint(valTmp), "'", "", -1)
			sqlValTmp = strings.Replace(sqlValTmp, "\"", "", -1)
			sqlValTmp = strings.Replace(sqlValTmp, "\\", "", -1)
		}
		if sqlk == "" {
			sqlk = "`" + columnType.ColumnName + "`"
			sqlv_ = "?"
			if columnType.DataType == "bit" {
				sqlval = sqlValTmp
			} else {
				sqlval = "'" + sqlValTmp + "'"
			}
		} else {
			sqlk += ",`" + columnType.ColumnName + "`"
			sqlv_ += ",?"
			if columnType.DataType == "bit" {
				sqlval += "," + sqlValTmp + ""
			} else {
				sqlval += ",'" + sqlValTmp + "'"
			}
		}
	}
	sqlstring = "INSERT INTO `" + schema + "`.`" + table + "` (" + sqlk + ") values (" + sqlv_ + ")"
	sqlstring2 := "INSERT INTO `" + schema + "`.`" + table + "` (" + sqlk + ") values (" + sqlval + ")"
	log.Println("sqlstring:", sqlstring2)
	log.Println("data len:", len(data))
	log.Println("columnData len:", len(columnData))
	return autoIncrementField, sqlstring, data, columnData, columnList
}

var ColumnData map[string]*Column
var table string
var database string
var longstring string
var autoCreate bool

func callback3(d *mysql.EventReslut) {
	if d.TableName != table {
		log.Println(d)
		return
	}

	if d.Query != "" {
		log.Println(d)
		return
	}

	checkData(d.Rows[len(d.Rows)-1], "binlog parser")
	proccessExit()
	os.Exit(0)
}

func checkData(rowMap map[string]interface{}, logPrefix string) (AutoIncrementField string, errorCheckDataList []string) {
	errorCheckDataList = make([]string, 0)
	var isAllRight bool = true
	for columnName, v := range rowMap {
		var logInfo string
		var isTrue = true
		if _, ok := ColumnData[columnName]; !ok {
			isAllRight = false
			logInfo = fmt.Sprintf(logPrefix+" columnName:", columnName, " not esxit")
			errDataList = append(errDataList, logInfo)
			continue
		}
		columnType := ColumnData[columnName]
		if columnType.AutoIncrement {
			AutoIncrementField = columnName
			log.Println(columnName, "==", v, " is AutoIncrement")
			continue
		}
		if columnType.Value == nil {
			if v == nil {
				log.Println(logPrefix, columnName, "==", v)
				continue
			} else {
				log.Println("columnType.Value:", nil)
				log.Println("v:", fmt.Sprint(v))
				isTrue = false
			}
		}
		switch columnType.DataType {
		case "set":
			setStr := strings.Replace(strings.Trim(fmt.Sprint(columnType.Value), "[]"), " ", ",", -1)
			var setStrVal string
			if reflect.TypeOf(v).Kind() == reflect.Slice {
				setStrVal = strings.Replace(strings.Trim(fmt.Sprint(v), "[]"), " ", ",", -1)
			} else {
				setStrVal = fmt.Sprint(v)
			}
			if setStr != setStrVal {
				isTrue = false
			}
			break
		case "json":
			c1, _ := json.Marshal(columnType.Value)
			var c2 []byte
			if reflect.TypeOf(v).Kind() != reflect.String {
				c2, _ = json.Marshal(v)
			} else {
				var d interface{}
				json.Unmarshal([]byte(v.(string)), &d)
				c2, _ = json.Marshal(d)
			}
			if string(c1) == string(c2) {
				log.Println(logPrefix, columnName, "==", v, "(", reflect.TypeOf(rowMap[columnName]), ")")
			} else {
				log.Println("columnType.Value:", string(c1))
				log.Println("v:", string(c2))
				isTrue = false
			}
			break
		case "tinyint":
			if fmt.Sprint(v) == fmt.Sprint(columnType.Value) {
				log.Println(logPrefix, columnName, "==", v)
			} else {
				isTrue = false
			}
			break
		default:
			if fmt.Sprint(v) == fmt.Sprint(columnType.Value) {
				log.Println(logPrefix, columnName, "==", v)
			} else {
				isTrue = false
			}
			break
		}
		if isTrue == false {
			isAllRight = false
			logInfo = fmt.Sprintf("dataType:(%s) %s %s value:%v ( %s ) != %v ( %s )", columnType.DataType, logPrefix, columnName, fmt.Sprint(rowMap[columnName]), reflect.TypeOf(rowMap[columnName]), ColumnData[columnName].Value, reflect.TypeOf(ColumnData[columnName].Value))
			errDataList = append(errDataList, logInfo)
		}
	}
	if isAllRight {
		errDataList = append(errDataList, logPrefix+" type and value is all right !!!")
	}
	return
}

func GetTableData(db mysql.MysqlConnection, SchemaName, TableName string, AutoIncrementField string, AutoIncrementValue []string, useStmt bool) ([]map[string]interface{}, error) {
	var where string
	var args = make([]driver.Value, 0)
	if AutoIncrementField != "" && len(AutoIncrementValue) > 0 {
		where = " AND " + AutoIncrementField + " in (?)"
		args = append(args, strings.Replace(strings.Trim(fmt.Sprint(AutoIncrementValue), "[]"), " ", "','", -1))
		//where = " AND " + AutoIncrementField + " in ('" + strings.Replace(strings.Trim(fmt.Sprint(AutoIncrementValue), "[]"), " ", "','", -1) + "')"
	} else {
		where = " LIMIT 1"
	}
	sql := "SELECT * FROM `" + SchemaName + "`.`" + TableName + "` WHERE  1=1 " + where
	var rows driver.Rows
	var err error
	if useStmt {
		rows, err = StmtSelect(db, sql, args)
	} else {
		rows, err = ExecQuery(db, sql, args)
	}

	if err != nil {
		log.Fatal(err)
	}
	fields := rows.Columns()
	n := len(fields)

	data := make([]map[string]interface{}, 0)
	for {
		m := make(map[string]interface{}, n)
		dest := make([]driver.Value, n, n)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		for i, fieldName := range fields {
			if dest[i] == nil {
				m[fieldName] = nil
				continue
			}
			columnInfo := ColumnData[fieldName]
			switch strings.ToLower(columnInfo.DataType) {
			case "time", "datetime", "timestamp":
				if columnInfo.Fsp == 0 {
					break
				}
				val := dest[i].(string)
				i := strings.Index(val, ".")
				if i < 0 {
					m[columnInfo.ColumnName] = val + "." + fmt.Sprintf("%0*d", columnInfo.Fsp, 0)
					break
				}
				n := len(val[i+1:])
				if n == columnInfo.Fsp {
					m[columnInfo.ColumnName] = val
					break
				}
				if n < columnInfo.Fsp {
					m[columnInfo.ColumnName] = val + fmt.Sprintf("%0*d", columnInfo.Fsp-n, 0)
				} else {
					m[columnInfo.ColumnName] = val[0 : len(val)-n+columnInfo.Fsp]
				}
				break
			case "set":
				m[fieldName] = strings.Split(fmt.Sprint(dest[i]), ",")
				break
			default:
				m[fieldName] = dest[i]
				break
			}
		}
		data = append(data, m)
	}

	return data, nil
}

func CheckSelectTableData(db mysql.MysqlConnection, SchemaName, TableName string, AutoIncrementField string, AutoIncrementValue []string, useStmt bool) {
	data, err := GetTableData(db, SchemaName, TableName, AutoIncrementField, AutoIncrementValue, useStmt)
	if err != nil {
		log.Fatal("CheckSelectTableData err:", err)
	}
	checkData(data[0], "select useStmt:"+fmt.Sprint(useStmt))
	return
}

type DataTypeSupportedStruct struct {
	Timestamp bool
	Json      bool
	Innodb    bool
}

func CheckVersionDataTypeSupportedByMysql(MysqlVersion string) (result *DataTypeSupportedStruct) {
	result = new(DataTypeSupportedStruct)
	reg, _ := regexp.Compile("[^0-9.]+")
	MysqlVersion = reg.ReplaceAllString(MysqlVersion, "")
	//MysqlVersion = strings.ReplaceAll(MysqlVersion,"-log","")
	MysqlVerionArr := strings.Split(MysqlVersion, ".")
	bigVersion, _ := strconv.Atoi(MysqlVerionArr[0])
	midVersion, _ := strconv.Atoi(MysqlVerionArr[1])
	var lastVersion = 0
	if len(MysqlVerionArr) == 3 {
		lastVersion, _ = strconv.Atoi(MysqlVerionArr[2])
	}
	mysqlVersionInt := bigVersion*1000 + midVersion*100 + lastVersion

	if mysqlVersionInt >= 5600 {
		result.Timestamp = true
	}
	if mysqlVersionInt >= 5700 {
		result.Json = true
		result.Innodb = true
	}
	return result
}

func CheckVersionDataTypeSupportedByMariaDB(version string) (result *DataTypeSupportedStruct) {
	result = new(DataTypeSupportedStruct)
	result.Timestamp = true
	if strings.Index(version, "10.0") == 0 || strings.Index(version, "10.1") == 0 {
		return
	}
	result.Json = true
	return
}

func CheckVersionDataTypeSupported(version string) *DataTypeSupportedStruct {
	version = strings.Trim(version, "")
	if strings.Contains(strings.ToLower(version), "mariadb") {
		return CheckVersionDataTypeSupportedByMariaDB(version)
	} else {
		return CheckVersionDataTypeSupportedByMysql(version)
	}
}

func main() {
	fmt.Println("VERSION:", VERSION)
	var userName, password, host, port string
	var CheckType int
	// 0  select binlog 都验证
	// 1  只验证 select
	// 2  只验证 binlog
	var IsGTID bool
	flag.StringVar(&userName, "u", "root", "-u root")
	flag.StringVar(&password, "p", "root", "-p password")
	flag.StringVar(&host, "h", "192.168.126.140", "-h 127.0.0.1")
	flag.StringVar(&port, "P", "3306", "-P 3306")
	flag.StringVar(&database, "database", "test", "-database test")
	flag.StringVar(&table, "table", "binlog_field_test", "-table binlog_field_test")
	flag.StringVar(&longstring, "longstring", "false", "-longstring true | true insert long text,SET GLOBAL max_allowed_packet = 4194304 ,please")
	flag.BoolVar(&autoCreate, "autoCreate", true, "-autoCreate")
	flag.IntVar(&CheckType, "CheckType", 0, "-CheckType")
	flag.BoolVar(&IsGTID, "IsGTID", true, "-IsGTID")

	flag.Parse()
	if autoCreate {
		if table == "" {
			table = "binlog_field_test"
		}
		if database == "" {
			database = "bifrost_test"
		}
	}

	var filename, dataSource string
	var position uint32 = 0
	var MyServerID uint32 = 0
	var MasterGtid string = ""

	dataSource = userName + ":" + password + "@tcp(" + host + ":" + port + ")/" + database
	log.Println(dataSource, " start connect")
	db := DBConnect(dataSource)
	if db == nil {
		log.Println("dataSource:", dataSource, " connect err")
		return
	}
	log.Println(dataSource, " start success")
	masterInfo := GetBinLogInfo(db)
	if masterInfo.File == "" {
		log.Println(dataSource, " binlog disabled")
		os.Exit(0)
	}

	filename = masterInfo.File
	position = uint32(masterInfo.Position)
	masterServerId := GetServerId(db)
	MyServerID = uint32(masterServerId + 250)

	MysqlVersion = GetMySQLVersion(db)
	if IsGTID {
		if strings.Contains(MysqlVersion, "MariaDB") {
			m := GetVariables(db, "gtid_binlog_pos")
			if gtidBinlogPos, ok := m["gtid_binlog_pos"]; ok {
				MasterGtid = gtidBinlogPos
			}
		} else {
			MasterGtid = masterInfo.Executed_Gtid_Set
		}
	}
	fmt.Println("server version:", MysqlVersion)

	fmt.Println("")

	createTableSql := "CREATE TABLE `" + database + "`.`" + table + "` (" +
		"`id` int(11) unsigned NOT NULL AUTO_INCREMENT," +
		"`testtinyint` tinyint(4) NOT NULL DEFAULT '-1'," +
		"`testsmallint` smallint(6) NOT NULL DEFAULT '-2'," +
		"`testmediumint` mediumint(8) NOT NULL DEFAULT '-3'," +
		"`testint` int(11) NOT NULL DEFAULT '-4'," +
		"`testbigint` bigint(20) NOT NULL DEFAULT '-5'," +
		"`testvarchar` varchar(10) NOT NULL," +
		"`testchar` char(2) NOT NULL," +
		"`testenum` enum('en1','en2','en3') NOT NULL DEFAULT 'en1'," +
		"`testset` set('set1','set2','set3') NOT NULL DEFAULT 'set1'," +
		"`testtime` time NOT NULL DEFAULT '00:00:00'," +
		"`testdate` date NOT NULL DEFAULT '0000-00-00'," +
		"`testyear` year(4) NOT NULL DEFAULT '1989'," +
		"`testtimestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
		"`testdatetime` datetime NOT NULL DEFAULT '0000-00-00 00:00:00'," +
		"`testfloat` float(9,2) NOT NULL DEFAULT '0.00'," +
		"`testdouble` double(9,2) NOT NULL DEFAULT '0.00'," +
		"`testdecimal` decimal(9,2) NOT NULL DEFAULT '0.00'," +
		"`testdecimal2` decimal(10,4) NOT NULL DEFAULT '0.00'," +
		"`testdecimal3` decimal(20,4) NOT NULL DEFAULT '0.00'," +
		"`testdecimal4` decimal(30,5) NOT NULL DEFAULT '0.00'," +
		"`testtext` text NOT NULL," +
		"`testblob` blob NOT NULL," +
		"`testbit` bit(8) NOT NULL DEFAULT b'0'," +
		"`testbool` tinyint(1) NOT NULL DEFAULT '0'," +
		"`testmediumblob` mediumblob NOT NULL," +
		"`testlongblob` longblob NOT NULL," +
		"`testtinyblob` tinyblob NOT NULL," +
		"`test_unsinged_tinyint` tinyint(4) unsigned NOT NULL DEFAULT '1'," +
		"`test_unsinged_smallint` smallint(6) unsigned NOT NULL DEFAULT '2'," +
		"`test_unsinged_mediumint` mediumint(8) unsigned NOT NULL DEFAULT '3'," +
		"`test_unsinged_int` int(11) unsigned NOT NULL DEFAULT '4'," +
		"`test_unsinged_bigint` bigint(20) unsigned NOT NULL DEFAULT '5'," +
		"`testtinyint_null` tinyint(4) DEFAULT NULL," +
		"`testsmallint_null` smallint(6) DEFAULT NULL," +
		"`testmediumint_null` mediumint(8) DEFAULT NULL," +
		"`testint_null` int(11) DEFAULT NULL," +
		"`testbigint_null` bigint(20) DEFAULT NULL," +
		"`testvarchar_null` varchar(10) DEFAULT NULL," +
		"`testchar_null` char(2) DEFAULT NULL," +
		"`testenum_null` enum('en1','en2','en3') DEFAULT NULL," +
		"`testset_null` set('set1','set2','set3') DEFAULT NULL," +
		"`testtime_null` time DEFAULT NULL," +
		"`testdate_null` date DEFAULT NULL," +
		"`testyear_null` year(4) DEFAULT NULL," +
		"`testtimestamp_null` timestamp NULL DEFAULT NULL," +
		"`testdatetime_null` datetime NULL DEFAULT NULL," +
		"`testfloat_null` float(9,2) DEFAULT NULL," +
		"`testdouble_null` double(9,2) DEFAULT NULL," +
		"`testdecimal_null` decimal(9,2) DEFAULT NULL," +
		"`testdecimal2_null` decimal(10,4) DEFAULT NULL," +
		"`testdecimal3_null` decimal(20,4) DEFAULT NULL," +
		"`testdecimal4_null` decimal(30,5) DEFAULT NULL," +
		"`testtext_null` text DEFAULT NULL," +
		"`testblob_null` blob DEFAULT NULL," +
		"`testbit_null` bit(8) DEFAULT NULL," +
		"`testbool_null` tinyint(1) DEFAULT NULL," +
		"`testmediumblob_null` mediumblob DEFAULT NULL," +
		"`testlongblob_null` longblob DEFAULT NULL," +
		"`testtinyblob_null` tinyblob DEFAULT NULL," +
		"`test_unsinged_tinyint_null` tinyint(4) unsigned DEFAULT NULL," +
		"`test_unsinged_smallint_null` smallint(6) unsigned DEFAULT NULL," +
		"`test_unsinged_mediumint_null` mediumint(8) unsigned DEFAULT NULL," +
		"`test_unsinged_int_null` int(11) unsigned DEFAULT NULL," +
		"`test_unsinged_bigint_null` bigint(20) unsigned DEFAULT NULL,"

	// 假如 mysql 版本 非 mysql5.7 及以上，不进行 json 类型测试

	dataTypeSupported := CheckVersionDataTypeSupported(MysqlVersion)

	if dataTypeSupported.Timestamp {
		createTableSql += "`testtime2_1` time(1) NULL DEFAULT NULL," +
			"`testtime2_2` time(2) NOT NULL," +
			"`testtime2_3` time(3) NOT NULL," +
			"`testtime2_4` time(4) NOT NULL," +
			"`testtime2_5` time(5) NOT NULL," +
			"`testtime2_6` time(6) NOT NULL," +
			"`testtimestamp2_1` timestamp(1) NOT NULL," +
			"`testtimestamp2_2` timestamp(2) NOT NULL," +
			"`testtimestamp2_3` timestamp(3) NOT NULL," +
			"`testtimestamp2_4` timestamp(4) NOT NULL," +
			"`testtimestamp2_5` timestamp(5) NOT NULL," +
			"`testtimestamp2_6` timestamp(6) NOT NULL," +
			"`testdatetime2_1` datetime(1) NOT NULL," +
			"`testdatetime2_2` datetime(2) NOT NULL," +
			"`testdatetime2_3` datetime(3) NOT NULL," +
			"`testdatetime2_4` datetime(4) NOT NULL," +
			"`testdatetime2_5` datetime(5) NOT NULL," +
			"`testdatetime2_6` datetime(6) NOT NULL,"

		createTableSql += "`testtime2_1_null` time(1) NULL DEFAULT NULL," +
			"`testtime2_2_null` time(2) NULL DEFAULT NULL," +
			"`testtime2_3_null` time(3) NULL DEFAULT NULL," +
			"`testtime2_4_null` time(4) NULL DEFAULT NULL," +
			"`testtime2_5_null` time(5) NULL DEFAULT NULL," +
			"`testtime2_6_null` time(6) NULL DEFAULT NULL," +
			"`testtimestamp2_1_null` timestamp(1) NULL DEFAULT NULL," +
			"`testtimestamp2_2_null` timestamp(2) NULL DEFAULT NULL," +
			"`testtimestamp2_3_null` timestamp(3) NULL DEFAULT NULL," +
			"`testtimestamp2_4_null` timestamp(4) NULL DEFAULT NULL," +
			"`testtimestamp2_5_null` timestamp(5) NULL DEFAULT NULL," +
			"`testtimestamp2_6_null` timestamp(6) NULL DEFAULT NULL," +
			"`testdatetime2_1_null` datetime(1) NULL DEFAULT NULL," +
			"`testdatetime2_2_null` datetime(2) NULL DEFAULT NULL," +
			"`testdatetime2_3_null` datetime(3) NULL DEFAULT NULL," +
			"`testdatetime2_4_null` datetime(4) NULL DEFAULT NULL," +
			"`testdatetime2_5_null` datetime(5) NULL DEFAULT NULL," +
			"`testdatetime2_6_null` datetime(6) NULL DEFAULT NULL,"
	}

	if dataTypeSupported.Json {
		createTableSql += "`test_json` json NOT NULL,"
		createTableSql += "`test_json_null` json NULL DEFAULT NULL,"
	}

	var engine = "MyISAM"
	if dataTypeSupported.Innodb {
		engine = "InnoDB"
	}

	createTableSql += "index testvarchar(testvarchar),PRIMARY KEY (`id`)" +
		") ENGINE=" + engine + " AUTO_INCREMENT=0 DEFAULT CHARSET=utf8 PARTITION BY HASH (id) PARTITIONS 3"

	log.Println("load data start")
	if autoCreate {
		var sqlList = []string{
			"CREATE DATABASE /*!32312 IF NOT EXISTS*/ `" + database + "`",
			"DROP TABLE IF EXISTS `" + database + "`.`" + table + "`",
			createTableSql,
		}

		log.Println("create table binlog_field_test start")
		for _, sql := range sqlList {
			log.Println("exec sql:", sql)
			ExecSQL(db, sql)
		}
		log.Println("create table binlog_field_test over")
		table = "binlog_field_test"
	}
	AutoIncrementField, sqlPre, sqlValue, tableInfo, columnList := GetSchemaTableFieldAndVal(db, database, table)
	if sqlPre == "" {
		log.Println("GetSchemaTableFieldAndVal ,sql is empty")
		os.Exit(0)
	}

	columnListByte, _ := json.Marshal(columnList)
	fmt.Println("columnListJson:", string(columnListByte))

	db.Exec("SET NAMES utf8", []driver.Value{})

	for k, v := range tableInfo {
		log.Println(k, "==", v.Value, "(", reflect.TypeOf(v.Value), ")")
	}
	ColumnData = tableInfo
	AutoIncrementValueArr := make([]string, 0)
	var AutoIncrementValue int64

	log.Println("exec insert starting")
	Result2, err := ExecInsert(db, sqlPre, sqlValue)
	if err != nil {
		log.Fatal(err)
	}
	// exec inert not supported get last insert id
	AutoIncrementValue = 1
	AutoIncrementValueArr = append(AutoIncrementValueArr, fmt.Sprint(AutoIncrementValue))
	log.Println("exec insert Result:", Result2)
	log.Println("exec insert end")

	if CheckType <= 1 {
		CheckSelectTableData(db, database, table, AutoIncrementField, AutoIncrementValueArr, false)
	}
	log.Println("exec data select check end")

	AutoIncrementValueArr = make([]string, 0)
	log.Println("stmt insert starting")
	Result, err := StmtInsert(db, sqlPre, sqlValue)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("stmt insert Result:", Result)
	log.Println("stmt insert end")
	AutoIncrementValue, err = Result.LastInsertId()
	if err != nil {
		log.Fatal(err)
	}
	AutoIncrementValueArr = append(AutoIncrementValueArr, fmt.Sprint(AutoIncrementValue))
	if CheckType <= 1 {
		CheckSelectTableData(db, database, table, AutoIncrementField, AutoIncrementValueArr, true)
	}
	log.Println("stmt data select check end")

	if CheckType == 1 {
		return
	}

	log.Println("load data over")

	defer proccessExit()

	reslut := make(chan error, 1)
	BinlogDump := mysql.NewBinlogDump(
		dataSource,
		callback3,
		[]mysql.EventType{
			mysql.QUERY_EVENT,
			mysql.WRITE_ROWS_EVENTv1, mysql.UPDATE_ROWS_EVENTv1, mysql.DELETE_ROWS_EVENTv1,
			mysql.WRITE_ROWS_EVENTv0, mysql.UPDATE_ROWS_EVENTv0, mysql.DELETE_ROWS_EVENTv0,
			mysql.WRITE_ROWS_EVENTv2, mysql.UPDATE_ROWS_EVENTv2, mysql.DELETE_ROWS_EVENTv2,
			mysql.XID_EVENT,
		},
		nil,
		nil)
	BinlogDump.AddReplicateDoDb(database, "binlog_field_test")
	log.Println("Version:", VERSION)
	log.Println("Bristol version:", mysql.VERSION)
	log.Println("filename:", filename, "position:", position, "MasterGtid:", MasterGtid)
	if MasterGtid != "" {
		go BinlogDump.StartDumpBinlogGtid(MasterGtid, MyServerID, reslut)
	} else {
		go BinlogDump.StartDumpBinlog(filename, position, MyServerID, reslut, "", 0)
	}
	go func() {
		for {
			v := <-reslut
			log.Printf("monitor reslut:%s \r\n", v)
			switch v.Error() {
			case "running", "starting":
				continue
			default:
				fmt.Println("mysql server:", MysqlVersion)
				os.Exit(1)
			}
		}
	}()
	for {
		time.Sleep(10 * time.Second)
	}
}

func StmtInsert(db mysql.MysqlConnection, sqlPre string, sqlValue []driver.Value) (driver.Result, error) {
	stmt, err := db.Prepare(sqlPre)
	if err != nil {
		log.Fatal(err, "sqlPre:", sqlPre)
	}
	defer stmt.Close()

	fmt.Println("")

	Result, err := stmt.Exec(sqlValue)
	if err != nil {
		log.Println(sqlValue)
		log.Fatal("sql Exec", err)
	}

	return Result, err
}

func StmtSelect(db mysql.MysqlConnection, sql string, args []driver.Value) (driver.Rows, error) {
	stmt, err := db.Prepare(sql)
	log.Println("StmtSelect sql:", sql)
	log.Println("StmtSelect args:", args)
	if err != nil {
		return nil, err
	}
	rows, err := stmt.Query(args)
	if err != nil {
		return nil, err
	}
	return rows, err
}

func ExecInsert(db mysql.MysqlConnection, sqlPre string, sqlValue []driver.Value) (driver.Result, error) {
	result, err := db.Exec(sqlPre, sqlValue)
	return result, err
}

func ExecQuery(db mysql.MysqlConnection, sql string, args []driver.Value) (driver.Rows, error) {
	return db.Query(sql, args)
}
