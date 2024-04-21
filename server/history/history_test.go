//go:build integration
// +build integration

package history_test

import (
	"encoding/json"
	"testing"
)
import (
	"database/sql/driver"
	"fmt"
	"github.com/brokercap/Bifrost/server/history"
	"log"
	"reflect"
	"strconv"
	"strings"
)

func TestGetDataList(t *testing.T) {
	historyObj := &history.History{
		DbName:     "test",
		SchemaName: "bifrost_test",
		TableName:  "bristol_performance_test",
		Status:     history.HISTORY_STATUS_CLOSE,
		NowStartI:  0,
		Property: history.HistoryProperty{
			ThreadNum:      1,
			ThreadCountPer: 10,
		},
		Uri: "root:@tcp(127.0.0.1:3306)/bifrost_test",
	}
	historyObj.Start()
}

func TestChekcDataType(t *testing.T) {
	SchemaName := "bifrost_test"
	TableName := "binlog_field_test2"
	Uri := "root:root@tcp(192.168.220.128:3308)/bifrost_test"
	db := history.DBConnect(Uri)
	Fields := history.GetSchemaTableFieldList(db, SchemaName, TableName)
	sql := "select * from `" + SchemaName + "`.`" + TableName + "` LIMIT 1"
	stmt, err := db.Prepare(sql)
	if err != nil {
		t.Fatal("Prepare err:", err)
		stmt.Close()
		return
	}
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	if err != nil {
		t.Fatal(err)
	}
	n := len(Fields)
	m := make(map[string]interface{}, n)
	for {
		dest := make([]driver.Value, n, n)
		err := rows.Next(dest)
		if err != nil {
			//log.Println("ssssssssff err:",err)
			break
		}
		for i, v := range Fields {
			if dest[i] == nil {
				m[*v.COLUMN_NAME] = nil
				continue
			}
			switch *v.DATA_TYPE {
			case "set":
				m[*v.COLUMN_NAME] = strings.Split(dest[i].(string), ",")
				break
			case "tinyint(1)":
				switch fmt.Sprint(dest[i]) {
				case "1":
					m[*v.COLUMN_NAME] = true
					break
				case "0":
					m[*v.COLUMN_NAME] = false
					break
				default:
					m[*v.COLUMN_NAME] = dest[i]
					break
				}
				break
			default:
				m[*v.COLUMN_NAME] = dest[i]
				break
			}
		}
		break
	}
	var noError bool = true

	for k, v := range m {
		switch k {
		case "id":
			switch v.(type) {
			case uint32:
				if v.(uint32) != 1 {
					t.Log(k, 1, "!=", v)
					noError = false
				} else {
					t.Log(k, 1, "==", v, "filed-Type:", "uint", "golang-type:", reflect.TypeOf(v), " is right")
				}
				break
			default:
				t.Error(k, 1, "!=", v, " type:", reflect.TypeOf(v))
				noError = false
			}
			break
		case "testtinyint":
			switch v.(type) {
			case int8:
				if v.(int8) != -1 {
					log.Println(k, -1, "!=", v)
					noError = false
				} else {
					log.Println(k, -1, "==", v, "filed-Type:", "tinyint", "golang-type:", reflect.TypeOf(v), " is right")
				}
				break
			default:
				log.Println(k, -1, "!=", v, " type:", reflect.TypeOf(v))
				noError = false
			}
			break
		case "testsmallint":
			switch v.(type) {
			case int16:
				if v.(int16) != -2 {
					log.Println(k, -2, "!=", v)
					noError = false
				} else {
					log.Println(k, -2, "==", v, "filed-Type:", "smallint", "golang-type:", reflect.TypeOf(v), " is right")
				}
				break
			default:
				log.Println(k, -3, "!=", v, " type:", reflect.TypeOf(v))
				noError = false
			}
			break
		case "testmediumint":
			switch v.(type) {
			case int32:
				if v.(int32) != -3 {
					log.Println(k, -3, "!=", v)
					noError = false
				} else {
					log.Println(k, -3, "==", v, "filed-Type:", "mediumint", "golang-type:", reflect.TypeOf(v), " is right")
				}
				break
			default:
				log.Println(k, -3, "!=", v, " type:", reflect.TypeOf(v))
				noError = false
			}
			break
		case "testint":
			switch v.(type) {
			case int32:
				if v.(int32) != -4 {
					log.Println(k, -4, "!=", v)
					noError = false
				} else {
					log.Println(k, -4, "==", v, "filed-Type:", "int", "golang-type:", reflect.TypeOf(v), " is right")
				}
				break
			default:
				log.Println(k, -4, "!=", v, " type:", reflect.TypeOf(v))
				noError = false
			}
			break
		case "testbigint":
			switch v.(type) {
			case int64:
				if v.(int64) != -5 {
					log.Println(k, -5, "!=", v)
					noError = false
				} else {
					log.Println(k, -5, "==", v, "filed-Type:", "bigint", "golang-type:", reflect.TypeOf(v), " is right")
				}
				break
			default:
				log.Println(k, -5, "!=", v, " type:", reflect.TypeOf(v))
				noError = false
			}
			break

		case "testvarchar":
			if v.(string) != "testvarcha" {
				log.Println(k, "testvarcha", "!=", v)
				noError = false
			} else {
				log.Println(k, "testvarcha", "==", v, "filed-Type:", "varchar", "golang-type:", reflect.TypeOf(v), " is right")
			}

			break
		case "testchar":
			if v.(string) != "te" {
				log.Println(k, "te", "!=", v)
				noError = false
			} else {
				log.Println(k, "te", "==", v, "filed-Type:", "char", "golang-type:", reflect.TypeOf(v), " is right")
			}
			break
		case "testenum":
			if v.(string) != "en2" {
				log.Println(k, "te", "!=", v)
				noError = false
			} else {
				log.Println(k, "en2", "==", v, "filed-Type:", "enum", "golang-type:", reflect.TypeOf(v), " is right")
			}
			break
		case "testset":
			f := v.([]string)
			var b bool = true
			if f[0] != "set1" && f[1] != "set1" {
				log.Println(k, "set1 no exsit", f)
				noError = false
				b = false
			}
			if f[1] != "set3" && f[0] != "set3" {
				log.Println(k, "set3 no exsit", f)
				noError = false
				b = false
			}
			if b == true {
				log.Println(k, "(set1,set3)", "==", v, "filed-Type:", "set", "golang-type:", reflect.TypeOf(v), " is right")
			}
			break
		case "testtime":
			if v.(string) != "15:39:59" {
				log.Println(k, "15:39:59", "!=", v)
				noError = false
			} else {
				log.Println(k, "15:39:59", "==", v, "filed-Type:", "time", "golang-type:", reflect.TypeOf(v), " is right")
			}
			break
		case "testdate":
			if v.(string) != "2018-05-08" {
				log.Println(k, "2018-05-08", "!=", v)
				noError = false
			} else {
				log.Println(k, "2018-05-08", "==", v, "filed-Type:", "date", "golang-type:", reflect.TypeOf(v), " is right")
			}
			break

		case "testyear":
			if v.(string) != "2018" {
				log.Println(k, "2018", "!=", v)
				noError = false
			} else {
				log.Println(k, "2018", "==", v, "filed-Type:", "year", "golang-type:", reflect.TypeOf(v), " is right")
			}
			break
		case "testtimestamp":
			if v.(string) != "2018-05-08 15:30:21" {
				log.Println(k, "2018-05-08 15:30:21", "!=", v)
				noError = false
			} else {
				log.Println(k, "2018-05-08 15:30:21", "==", v, "filed-Type:", "timestamp", "golang-type:", reflect.TypeOf(v), " is right")
			}
			break
		case "testdatetime":
			if v.(string) != "2018-05-08 15:30:21" {
				log.Println(k, "2018-05-08 15:30:21", "!=", v)
				noError = false
			} else {
				log.Println(k, "2018-05-08 15:30:21", "==", v, "filed-Type:", "datetime", "golang-type:", reflect.TypeOf(v), " is right")
			}
			break
		case "testfloat":
			if v.(float32) != 9.39 {
				log.Println(k, 9.39, "!=", v)
				noError = false
			} else {
				log.Println(k, 9.39, "==", v, "filed-Type:", "float", "golang-type:", reflect.TypeOf(v), " is right")
			}
			break
		case "testdouble":
			if v.(float64) != 9.39 {
				log.Println(k, 9.39, "!=", v)
				noError = false
			} else {
				log.Println(k, 9.39, "==", v, "filed-Type:", "double", "golang-type:", reflect.TypeOf(v), " is right")
			}
			break

		case "testdecimal":
			if v.(string) != "9.39" {
				log.Println(k, 9.39, "!=", v)
				noError = false
			} else {
				log.Println(k, 9.39, "==", v, "filed-Type:", "decimal", "golang-type:", reflect.TypeOf(v), " is right")
			}
			break

		case "testtext":
			if v.(string) != "testtext" {
				log.Println(k, "testtext", "!=", v)
				noError = false
			} else {
				log.Println(k, "testtext", "==", v, "filed-Type:", "text", "golang-type:", reflect.TypeOf(v), " is right")
			}
			break

		case "testblob":
			if v.(string) != "testblob" {
				log.Println(k, "testblob", "!=", v)
				noError = false
			} else {
				log.Println(k, "testblob", "==", v, "filed-Type:", "blob", "golang-type:", reflect.TypeOf(v), " is right")
			}
			break

		case "testbit":
			switch v.(type) {
			case int64:
				if v.(int64) != 8 {
					log.Println(k, 8, "!=", v)
					noError = false
				} else {
					log.Println(k, "8", "==", v, "filed-Type:", "bit", "golang-type:", reflect.TypeOf(v), " is right")
				}
				break
			default:
				log.Println(k, 8, "!=", v, " type:", reflect.TypeOf(v))
				noError = false
			}
			break

		case "testbool":

			switch v.(type) {
			case bool:
				if v.(bool) != true {
					t.Error(k, "true", "!=", v)
					noError = false
				} else {
					t.Log(k, "true", "==", v, "filed-Type:", "bool", "golang-type:", reflect.TypeOf(v), " is right")
				}
				break
			default:
				t.Log(k, "true", "!=", v, " type:", reflect.TypeOf(v))
				noError = false
			}
			break

		case "testmediumblob":
			if v.(string) != "testmediumblob" {
				t.Error(k, "testmediumblob", "!=", v)
				noError = false
			} else {
				t.Log(k, "testmediumblob", "==", v, "filed-Type:", "mediumblob", "golang-type:", reflect.TypeOf(v), " is right")
			}
			break

		case "testlongblob":
			if v.(string) != "testlongblob" {
				t.Error(k, "testlongblob", "!=", v)
				noError = false
			} else {
				t.Log(k, "testlongblob", "==", v, "filed-Type:", "longblob", "golang-type:", reflect.TypeOf(v), " is right")
			}
			break

		case "testtinyblob":
			if v.(string) != "testtinyblob" {
				t.Error(k, "testtinyblob", "!=", v)
				noError = false
			} else {
				t.Log(k, "testtinyblob", "==", v, "filed-Type:", "tinyblob", "golang-type:", reflect.TypeOf(v), " is right")
			}
			break

		case "test_unsinged_tinyint":
			switch v.(type) {
			case uint8:
				if v.(uint8) != 1 {
					t.Error(k, 1, "!=", v)
					noError = false
				} else {
					t.Log(k, "1", "==", v, "filed-Type:", "unsinged_tinyint", "golang-type:", reflect.TypeOf(v), " is right")
				}
				break
			default:
				t.Error(k, 1, "!=", v, " type:", reflect.TypeOf(v))
				noError = false
			}

			break

		case "test_unsinged_smallint":
			switch v.(type) {
			case uint16:
				if v.(uint16) != 2 {
					t.Error(k, 2, "!=", v)
					noError = false
				} else {
					t.Log(k, "2", "==", v, "filed-Type:", "unsinged_smallint", "golang-type:", reflect.TypeOf(v), " is right")
				}
				break
			default:
				t.Error(k, 2, "!=", v, " type:", reflect.TypeOf(v))
				noError = false
			}
			break

		case "test_unsinged_mediumint":
			switch v.(type) {
			case uint32:
				if v.(uint32) != 3 {
					t.Error(k, 3, "!=", v)
					noError = false
				} else {
					t.Log(k, "3", "==", v, "filed-Type:", "unsinged_mediumint", "golang-type:", reflect.TypeOf(v), " is right")
				}
				break
			default:
				t.Error(k, 3, "!=", v, " type:", reflect.TypeOf(v))
				noError = false
			}
			break

		case "test_unsinged_int":
			switch v.(type) {
			case uint32:
				if v.(uint32) != 4 {
					t.Error(k, 4, "!=", v)
					noError = false
				} else {
					t.Log(k, "4", "==", v, "filed-Type:", "unsinged_int", "golang-type:", reflect.TypeOf(v), " is right")
				}
				break
			default:
				t.Error(k, 4, "!=", v, " type:", reflect.TypeOf(v))
				noError = false
			}
			break

		case "test_unsinged_bigint":
			switch v.(type) {
			case uint64:
				if v.(uint64) != 5 {
					t.Error(k, 5, "!=", v)
					noError = false
				} else {
					t.Log(k, "5", "==", v, "filed-Type:", "unsinged_bigint", "golang-type:", reflect.TypeOf(v), " is right")
				}
				break
			default:
				t.Error(k, 5, "!=", v, " type:", reflect.TypeOf(v))
				noError = false
			}
			break
		case "testjson":
			switch v.(type) {
			case string:
				var d interface{}
				json.Unmarshal([]byte(v.(string)), &d)
				switch reflect.ValueOf(d).Kind() {
				case reflect.Map, reflect.Slice, reflect.Array:
					t.Log(k, "data:", d, " is json is right,")
					break
				default:
					t.Error(k, 5, "!=", v, " type:", reflect.TypeOf(v))
					break
				}
				break
			default:
				t.Error(k, 5, "!=", v, " type:", reflect.TypeOf(v))
			}

		default:
			t.Error(k, ":", v, " error type")
			noError = false
		}
	}

	if noError == true {
		t.Log(" type and value is all right ")
	} else {
		t.Fatal(" test failed")
	}
}

func TestCreateSQL(t *testing.T) {
	This := &history.History{
		DbName:     "test",
		SchemaName: "bifrost_test",
		TableName:  "bristol_performance_test",
		Status:     history.HISTORY_STATUS_CLOSE,
		NowStartI:  0,
		Property: history.HistoryProperty{
			ThreadNum:      1,
			ThreadCountPer: 10,
			Where:          " id > 0 ",
		},
		Uri: "root:@tcp(127.0.0.1:3306)/bifrost_test",
	}
	start := 0
	TablePriKey := "id"
	var sql = ""
	var where string = ""
	if This.Property.Where != "" {
		where = " WHERE " + This.Property.Where
	}
	var limit string = ""
	limit = " LIMIT " + strconv.Itoa(start) + "," + strconv.Itoa(This.Property.ThreadCountPer)
	if TablePriKey == "" {
		sql = "select * from `" + This.SchemaName + "`.`" + This.TableName + "`" + where + limit
		//sql := "select * from ? LIMIT ?,?"
	} else {
		sql = "select a.* from `" + This.SchemaName + "`.`" + This.TableName + "` as a "
		sql += " inner join ("
		sql += " select " + TablePriKey + " from `" + This.SchemaName + "`.`" + This.TableName + "`" + where + limit
		sql += " ) as b"
		sql += " on a." + TablePriKey + " = b." + TablePriKey
	}

	t.Log(sql)
}
