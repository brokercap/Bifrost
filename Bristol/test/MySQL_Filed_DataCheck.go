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
	"strconv"
	"strings"
	"time"
)

const VERSION = "1.3.0"

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
		dest := make([]driver.Value, 4, 4)
		errs := rows.Next(dest)
		if errs != nil {
			return MasterBinlogInfoStruct{}
		}
		File = dest[0].(string)
		Binlog_Do_DB = dest[2].(string)
		Binlog_Ignore_DB = dest[3].(string)
		Executed_Gtid_Set = ""
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

func GetSchemaTableFieldAndVal(db mysql.MysqlConnection, schema string, table string) (sqlstring string, data []driver.Value, columnData map[string]*Column, ColumnList []Column) {
	sql := "SELECT COLUMN_NAME,COLUMN_KEY,COLUMN_TYPE,CHARACTER_SET_NAME,COLLATION_NAME,NUMERIC_SCALE,EXTRA,COLUMN_DEFAULT,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION FROM `information_schema`.`COLUMNS` WHERE TABLE_SCHEMA = '" + schema + "' AND  table_name = '" + table + "'"
	data = make([]driver.Value, 0)
	stmt, err := db.Prepare(sql)
	columnList := make([]Column, 0)
	if err != nil {
		log.Println(err)
		return "", make([]driver.Value, 0), columnData, columnList
	}
	p := make([]driver.Value, 0)
	//p = append(p,schema)
	//p = append(p,table)
	rows, err := stmt.Query(p)
	defer rows.Close()
	if err != nil {
		log.Printf("%v\n", err)
		return "", make([]driver.Value, 0), columnData, columnList
	}
	columnData = make(map[string]*Column, 0)
	var sqlk, sqlv = "", ""
	for {
		dest := make([]driver.Value, 11, 11)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		var COLUMN_NAME, COLUMN_KEY, COLUMN_TYPE string
		var CHARACTER_SET_NAME, COLLATION_NAME, EXTRA string
		var NUMERIC_SCALE int
		var isBool = false
		var unsigned = false
		var is_primary = false
		var auto_increment = false
		var enum_values, set_values []string
		var COLUMN_DEFAULT string
		var DATA_TYPE string
		var CHARACTER_MAXIMUM_LENGTH int
		var NUMERIC_PRECISION int

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
		} else {
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
							Value = uint64(uint64(2) ^ 64 - 1)
						} else {
							if randResult == 1 {
								Value = int64(int64(2) ^ 63 - 1)
							} else {
								Value = int64(int64(-2) ^ 63)
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
				var enSize, cnSize = 0, 0
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
				var enSize, cnSize = 0, 0
				rand.Seed(time.Now().UnixNano())

				var n int
				if *longstring == "true" {
					if columnType.ColumnType == "longblob" {
						n = rand.Intn(65535 / 4)
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
				Value := time.Now().Format("15:04:05")
				columnType.Value = Value
				data = append(data, Value)
				break
			case "date":
				Value := time.Now().Format("2006-01-02")
				columnType.Value = Value
				data = append(data, Value)
				break
			case "datetime", "timestamp":
				Value := time.Now().Format("2006-01-02 15:04:05")
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
				Value := strconv.FormatFloat(float64(rand.Float64()), 'f', 2, 64)
				Value2, _ := strconv.ParseFloat(Value, 64)
				f1 := float64(rand.Intn(999999))
				f2 := f1 + float64(Value2)
				if randResult == 1 {
					f2 = 0 - f2
				}
				columnType.Value = fmt.Sprint(f2)
				data = append(data, fmt.Sprint(f2))
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
				m["key1"] = make([]interface{}, 0)
				m["key1"] = append(m["key1"], 2147483647)
				m["key1"] = append(m["key1"], -2147483648)
				m["key1"] = append(m["key1"], "2")
				m["key1"] = append(m["key1"], nil)
				m["key1"] = append(m["key1"], true)
				m["key1"] = append(m["key1"], "我是一个中国人,我爱中国！")
				c, _ := json.Marshal(m)
				columnType.Value = m
				data = append(data, string(c))
				break
			default:
				data = append(data, "0")
				break
			}

			if sqlk == "" {
				sqlk = "`" + columnType.ColumnName + "`"
				sqlv = "?"
			} else {
				sqlk += ",`" + columnType.ColumnName + "`"
				sqlv += ",?"
			}
		}
	}
	sqlstring = "INSERT INTO `" + schema + "`.`" + table + "` (" + sqlk + ") values (" + sqlv + ")"
	log.Println("sqlstring:", sqlstring)
	log.Println("data:", len(data))
	log.Println("columnData:", len(columnData))
	return sqlstring, data, columnData, columnList
}

var ColumnData map[string]*Column
var table *string
var database *string
var longstring *string

func callback3(d *mysql.EventReslut) {
	if d.TableName != *table {
		log.Println(d)
		return
	}

	if d.Query != "" {
		log.Println(d)
		return
	}

	var AutoIncrementField = ""

	var isAllRight = true
	errorFieldList := make([]string, 0)
	for columnName, v := range d.Rows[len(d.Rows)-1] {
		if _, ok := ColumnData[columnName]; !ok {
			log.Println("columnName:", columnName, " not esxit")
			continue
		}
		columnType := ColumnData[columnName]
		if columnType.AutoIncrement {
			AutoIncrementField = columnName
			log.Println(columnName, "==", v, " is AutoIncrement")
			continue
		}

		if columnType.DataType == "json" || reflect.TypeOf(v) == reflect.TypeOf(columnType.Value) {
			if fmt.Sprint(v) == fmt.Sprint(columnType.Value) {
				log.Println(columnName, "==", v)
			} else {
				isAllRight = false
				errorFieldList = append(errorFieldList, columnName)
				//log.Println(columnName,"value:",v,"(",reflect.TypeOf(v),")"," != ",columnType.Value,"(",reflect.TypeOf(columnType.Value),")"+ " type is right")
			}
		} else {
			isAllRight = false
			errorFieldList = append(errorFieldList, columnName)
			//log.Println(columnName,"value:",v,"(",reflect.TypeOf(v),")"," != ",columnType.Value,"(",columnType.Value,")"+ " type is error")
		}

	}

	if isAllRight == true {
		fmt.Println("")
		if AutoIncrementField != "" {
			log.Println(AutoIncrementField, "==", d.Rows[len(d.Rows)-1][AutoIncrementField])
		}
		log.Println(" type and value is all right ")
	} else {
		for _, columnName := range errorFieldList {
			log.Println(columnName, "value:", d.Rows[len(d.Rows)-1][columnName], "(", reflect.TypeOf(d.Rows[len(d.Rows)-1][columnName]), ")", " != ", ColumnData[columnName].Value, "(", reflect.TypeOf(ColumnData[columnName].Value), ")")
		}
	}
	os.Exit(0)
}

func main() {

	fmt.Println("VERSION:", VERSION)
	userName := flag.String("u", "root", "-u root")
	password := flag.String("p", "root", "-p password")
	host := flag.String("h", "127.0.0.1", "-h 127.0.0.1")
	port := flag.String("P", "3306", "-P 3306")
	database = flag.String("database", "test", "-database test")
	table = flag.String("table", "binlog_field_test", "-table binlog_field_test")
	longstring = flag.String("longstring", "false", "-longstring true | true insert long text,SET GLOBAL max_allowed_packet = 4194304 ,please")
	flag.Parse()

	var filename, dataSource string
	var position uint32 = 0
	var MyServerID uint32 = 0

	dataSource = *userName + ":" + *password + "@tcp(" + *host + ":" + *port + ")/" + *database
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

	stmt0, err := db.Prepare("select version()")
	rows0, _ := stmt0.Query([]driver.Value{})
	var MysqlVersion string
	for {
		dest := make([]driver.Value, 1, 1)

		err := rows0.Next(dest)
		if err != nil {
			break
		}
		MysqlVersion = fmt.Sprint(dest[0])
		break
	}

	fmt.Println("mysql version:", MysqlVersion)

	fmt.Println("")

	log.Println("load data start")
	if *table == "" {
		var sqlList = []string{
			//"CREATE DATABASE /*!32312 IF NOT EXISTS*/ `jc3wish_test`",
			"DROP TABLE IF EXISTS `" + *database + "`.`binlog_field_test`",
			"CREATE TABLE `" + *database + "`.`binlog_field_test` (" +
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
				"`test_json` json," +
				"PRIMARY KEY (`id`)" +
				") ENGINE=MyISAM AUTO_INCREMENT=0 DEFAULT CHARSET=utf8",
		}
		// 假如 mysql 版本 非 mysql5.7 及以上，不进行 json 类型测试
		bigVersionString := strings.Split(MysqlVersion, ".")[0]
		fmt.Println("bigVersionString:", bigVersionString)
		bigVersion, _ := strconv.Atoi(bigVersionString)
		fmt.Println("MysqlVersion[0:2]:", MysqlVersion[0:2])
		if bigVersion < 8 && MysqlVersion[0:2] != "5.7" {
			sqlList[1] = "CREATE TABLE `" + *database + "`.`binlog_field_test` (" +
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
				"PRIMARY KEY (`id`)" +
				") ENGINE=MyISAM AUTO_INCREMENT=0 DEFAULT CHARSET=utf8"
		}

		log.Println("create table binlog_field_test start")
		for _, sql := range sqlList {
			log.Println("exec sql:", sql)
			ExecSQL(db, sql)
		}
		log.Println("create table binlog_field_test over")
		*table = "binlog_field_test"
	}
	sqlPre, sqlValue, tableInfo, columnList := GetSchemaTableFieldAndVal(db, *database, *table)
	if sqlPre == "" {
		log.Println("GetSchemaTableFieldAndVal ,sql is empty")
		os.Exit(0)
	}

	columnListByte, _ := json.Marshal(columnList)
	fmt.Println("columnListJson:", string(columnListByte))

	db.Exec("SET NAMES utf8", []driver.Value{})
	stmt, err := db.Prepare(sqlPre)
	if err != nil {
		log.Fatal(err, "sqlPre:", sqlPre)
	}
	for k, v := range tableInfo {
		log.Println(k, "==", v.Value, "(", reflect.TypeOf(v.Value), ")")
	}

	fmt.Println("")

	/*
		for k,v:=range sqlValue{
			log.Println(k,"==",v,"(",reflect.TypeOf(v),")")
		}
	*/

	Result, err := stmt.Exec(sqlValue)
	if err != nil {
		log.Println(sqlValue)
		log.Fatal("sql Exec", err)
	}
	log.Println("sql exec ResultL:", Result)
	log.Println("load data over")

	ColumnData = tableInfo

	reslut := make(chan error, 1)
	BinlogDump := mysql.NewBinlogDump(
		dataSource,
		callback3,
		[]mysql.EventType{
			mysql.QUERY_EVENT,
			mysql.WRITE_ROWS_EVENTv1, mysql.UPDATE_ROWS_EVENTv1, mysql.DELETE_ROWS_EVENTv1,
			mysql.WRITE_ROWS_EVENTv0, mysql.UPDATE_ROWS_EVENTv0, mysql.DELETE_ROWS_EVENTv0,
			mysql.WRITE_ROWS_EVENTv2, mysql.UPDATE_ROWS_EVENTv2, mysql.DELETE_ROWS_EVENTv2,
		},
		nil,
		nil)
	BinlogDump.AddReplicateDoDb(*database, "binlog_field_test")
	log.Println("Version:", VERSION)
	log.Println("Bristol version:", mysql.VERSION)
	log.Println("filename:", filename, "position:", position)
	go BinlogDump.StartDumpBinlog(filename, position, MyServerID, reslut, "", 0)
	go func() {
		for {
			v := <-reslut
			log.Printf("monitor reslut:%s \r\n", v)
			if v.Error() == "close" {
				os.Exit(1)
			}
		}
	}()
	for {
		time.Sleep(10 * time.Second)
	}
}
