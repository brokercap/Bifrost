package src_test

import "testing"

import (
	"database/sql/driver"
	"fmt"
	MyPlugin "github.com/brokercap/Bifrost/plugin/clickhouse/src"
	"math"
	"os"
	"reflect"
	"strconv"
	"time"
)

func TestAllTypeToInt64(t *testing.T) {
	data := "2019"
	i64, err := MyPlugin.AllTypeToInt64(data)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(i64)

	ui64, err2 := MyPlugin.AllTypeToUInt64(data)
	if err2 != nil {
		t.Fatal(err2)
	}

	t.Log(ui64)
}

func TestFloat(t *testing.T) {
	v := float64(0.3)
	v2 := "0.30"
	floatDest, _ := strconv.ParseFloat(fmt.Sprint(v), 64)
	floatSource, _ := strconv.ParseFloat(fmt.Sprint(v2), 64)
	if math.Abs(floatDest-floatSource) < 0.05 {
		t.Log("test success")
	} else {
		t.Error("test failed")
	}
}

func TestCkDataTypeTransfer(t *testing.T) {
	var data string = "132423　"
	var fieldName string
	var toDataType string
	fieldName = "testField"
	toDataType = "Int64"
	t.Log("test start")
	result, err := MyPlugin.CkDataTypeTransfer(data, fieldName, toDataType, false)
	if err != nil {
		t.Fatal(err)
	}
	if reflect.TypeOf(result).String() == "int64" {
		if result.(int64) == int64(132423) {
			t.Log("result(int64):", result)
		} else {
			t.Fatal("result:", result, "(", reflect.TypeOf(result), ")")
		}
	} else {
		t.Fatal("result:", result, "(", reflect.TypeOf(result), ")")
	}

	toDataType = "UInt32"
	result, err = MyPlugin.CkDataTypeTransfer(data, fieldName, toDataType, false)
	if err != nil {
		t.Fatal(err)
	}
	if reflect.TypeOf(result).String() == "uint32" {
		if result.(uint32) == uint32(132423) {
			t.Log("result(uint32):", result)
		} else {
			t.Fatal("result:", result, "(", reflect.TypeOf(result), ")")
		}
	} else {
		t.Fatal("result:", result, "(", reflect.TypeOf(result), ")")
	}

	data = "42342.224 "
	toDataType = "Float32"
	result, err = MyPlugin.CkDataTypeTransfer(data, fieldName, toDataType, false)
	if err != nil {
		t.Fatal(err)
	}
	if reflect.TypeOf(result).String() == "float32" {
		if result.(float32) == float32(42342.224) {
			t.Log("result(float32):", result)
		} else {
			t.Fatal("result:", result, "(", reflect.TypeOf(result), ")")
		}
	} else {
		t.Fatal("result:", result, "(", reflect.TypeOf(result), ")")
	}

	toDataType = "Float64"
	result, err = MyPlugin.CkDataTypeTransfer(data, fieldName, toDataType, false)
	if err != nil {
		t.Fatal(err)
	}
	if reflect.TypeOf(result).String() == "float64" {
		if result.(float64) == float64(42342.224) {
			t.Log("result(float32):", result)
			os.Exit(0)
		} else {
			t.Fatal("result:", result, "(", reflect.TypeOf(result), ")")
		}
	} else {
		t.Fatal("result:", result, "(", reflect.TypeOf(result), ")")
	}

	toDataType = "Decimal(18, 2)"
	result, err = MyPlugin.CkDataTypeTransfer(data, fieldName, toDataType, false)
	if err != nil {
		t.Fatal(err)
	}
	if reflect.TypeOf(result).String() == "float64" {
		if result.(float64) == float64(42342.224) {
			t.Log("result(float64):", result)
		} else {
			t.Fatal("result:", result, "(", reflect.TypeOf(result), ")")
		}
	} else {
		t.Fatal("result:", result, "(", reflect.TypeOf(result), ")")
	}

}

func TestTransferToCreateTableSql(t *testing.T) {
	/*data := pluginTestData.NewEvent().GetTestInsertData()

	obj := &MyPlugin.Conn{
		p: &MyPlugin.PluginParam{
			CkSchema:    "",
			ModifDDLMap: map[string]bool{},
		},
	}
	obj.p.ModifDDLMap["ColumnAdd"] = true
	obj.p.ModifDDLMap["ColumnModify"] = true
	obj.p.ModifDDLMap["ColumnChange"] = true
	obj.p.ModifDDLMap["ColumnDrop"] = true
	obj.p.ModifDDLMap["TableRename"] = true
	obj.p.CkEngine = 2
	obj.p.CkClusterName = "ck_cluster"

	sql, distributeSql, viewSql, ckField := obj.TransferToCreateTableSql(data)
	t.Log(sql)
	t.Log(distributeSql)
	t.Log(viewSql)
	t.Log(ckField)
	c := MyPlugin.NewClickHouseDBConn(url)
	err := c.Exec(sql, []driver.Value{})
	if err != nil {
		t.Fatal(err)
	}

	err = c.Exec(distributeSql, []driver.Value{})
	if err != nil {
		t.Fatal(err)
	}

	err = c.Exec(viewSql, []driver.Value{})
	if err != nil {
		t.Fatal(err)
	}

	t.Log("success")*/
}

func TestTransferToCreateDatabaseSql(t *testing.T) {
	obj := &MyPlugin.Conn{}
	sql := obj.TransferToCreateDatabaseSql("mytest2")
	t.Log(sql)
	c := MyPlugin.NewClickHouseDBConn(url)
	err := c.Exec(sql, []driver.Value{})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("success")
}

func TestDateStringTransfer(t *testing.T) {

	var f = func(str string) (string, error) {
		var err error
		var time0 time.Time
		var Result string = "String"
		var n = len(str)
		switch n {
		case 19:
			time0, err = time.Parse("2006-01-02 15:04:05", str)
			if err != nil {
				return "String", err
			}
			Result = "DateTime"
			t.Log(time0.String())
			break
		case 10:
			time0, err = time.Parse("2006-01-02", str)
			if err != nil {
				return "String", err
			}
			Result = "Date"
			t.Log(time0.String())
			break
		default:
			if n > 19 && n <= 26 {
				nsec := fmt.Sprintf("%0*d", n-20, 0)
				time0, err = time.Parse("2006-01-02 15:04:05."+nsec, str)
				if err != nil {
					return "String", err
				}
			} else {
				return "String", err
			}
			Result = "DateTime64"
			t.Log(time0.String())
			break
		}
		return Result, err
	}

	type result struct {
		Val   string
		Type  string
		IsErr bool
	}

	testArr := make([]result, 0)
	testArr = append(testArr, result{Val: "2006-01-08", Type: "Date"})
	testArr = append(testArr, result{Val: "2006-01-08 00:05:20", Type: "DateTime"})
	testArr = append(testArr, result{Val: "00:05:20", Type: "String"})
	testArr = append(testArr, result{Val: "2006-01-32 00:05:20", Type: "String", IsErr: true})
	testArr = append(testArr, result{Val: "2006-01-08 00:05:20.123000", Type: "DateTime64"})
	testArr = append(testArr, result{Val: "2006-01-08 00:05:20.123", Type: "DateTime64"})

	for _, v := range testArr {
		TypeName, err := f(v.Val)
		if err != nil && !v.IsErr {
			t.Error(v.Val, "err:", err)
			continue
		}
		if TypeName != v.Type {
			t.Error(v.Val, TypeName, "!=", v.Type, " ( need )")
			continue
		}
		t.Log(v.Val, v.Type, "success")
	}
}

func TestGetDateTimeWithMs(t *testing.T) {
	var f = func(nsec int) string {
		var format string
		if nsec <= 0 {
			format = "2006-01-02 15:03:04"
		} else {
			format = "2006-01-02 15:03:04." + fmt.Sprintf("%0*d", nsec, 0)
		}
		t.Log("format:", format)
		return time.Now().Format(format)
	}
	for i := 0; i < 7; i++ {
		t.Log(f(i))
	}
}

func TestDateTimeWithMsFormat(t *testing.T) {
	var str = "2020-12-05 17:16:09.160"
	time0, err := time.Parse("2006-01-02 15:04:05.000", str)
	if err != nil {
		t.Fatal(err)
	}
	timeStr := time0.Format("2006-01-02 15:04:05.000")
	if timeStr != str {
		t.Fatal("error timeStr", timeStr, " != ", str)
	}
	t.Log("timeStr == ", timeStr)
}

func TestConn_TransferToCkTypeByColumnType(t *testing.T) {
	type result struct {
		Val   string
		Type  string
		IsErr bool
	}

	testArr := make([]result, 0)
	testArr = append(testArr, result{Val: "date", Type: "Date"})
	testArr = append(testArr, result{Val: "Nullable(timestamp(5))", Type: "Nullable(DateTime64(5))"})
	testArr = append(testArr, result{Val: "time(5)", Type: "String"})
	testArr = append(testArr, result{Val: "timestamp(5)", Type: "DateTime64(5)"})
	testArr = append(testArr, result{Val: "datetime(5)", Type: "DateTime64(5)"})
	testArr = append(testArr, result{Val: "Nullable(datetime(5))", Type: "Nullable(DateTime64(5))"})
	testArr = append(testArr, result{Val: "uint64", Type: "UInt64"})
	testArr = append(testArr, result{Val: "decimal(3, 2)", Type: "Decimal(3,2)"})
	testArr = append(testArr, result{Val: "Nullable(decimal( 18, 5))", Type: "Nullable(Decimal(18,5))"})
	testArr = append(testArr, result{Val: "Nullable(decimal( 38, 5))", Type: "Nullable(String)"})
	testArr = append(testArr, result{Val: "decimal( )", Type: "Decimal(18,2)"})
	testArr = append(testArr, result{Val: "decimal(1)", Type: "Decimal(1,0)"})

	conn := &MyPlugin.Conn{}
	for _, v := range testArr {
		TypeName := conn.TransferToCkTypeByColumnType(v.Val, true)
		if TypeName != v.Type {
			t.Error(v.Val, TypeName, "!=", v.Type, " ( need )")
			continue
		}
		t.Log(v.Val, v.Type, "success")
	}
}

func TestTransferNotes2Space(t *testing.T) {
	var sql string
	sql = `ALTER TABLE /* it is notes */ binlog_field_test 
  CHANGE testtinyint testtinyint INT UNSIGNED DEFAULT -1  NOT NULL,
  CHANGE testvarchar testvarchar VARCHAR(60) CHARSET utf8 COLLATE utf8_general_ci NOT NULL,
  ADD COLUMN testint2 INT(11) DEFAULT 0  NOT NULL   COMMENT 'test ok' AFTER test_json,
  MODIFY COLUMN testint3 int DEFAULT 1 NULL comment 'sdfsdf sdf',`
	sql = MyPlugin.TransferNotes2Space(sql)
	t.Log(sql)
}
