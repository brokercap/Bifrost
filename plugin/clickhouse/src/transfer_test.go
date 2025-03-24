package src

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestAllTypeToInt64(t *testing.T) {
	data := "2019"
	i64, err := AllTypeToInt64(data)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(i64)

	ui64, err2 := AllTypeToUInt64(data)
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
	fieldName := "testField"
	type testCase struct {
		Source             interface{}
		Nullable           bool
		ToDataType         string
		Result             interface{}
		ToDataTypeInMomery string
	}
	var testCaseList = []testCase{
		{
			Source:             "132423　",
			Nullable:           false,
			ToDataType:         "Int64",
			Result:             int64(132423),
			ToDataTypeInMomery: "int64",
		},
		{
			Source:             "132423　",
			Nullable:           false,
			ToDataType:         "UInt32",
			Result:             uint32(132423),
			ToDataTypeInMomery: "uint32",
		},
		{
			Source:             "42342.224 ",
			Nullable:           false,
			ToDataType:         "Float32",
			Result:             float32(42342.224),
			ToDataTypeInMomery: "float32",
		},
		{
			Source:             "42342.224 ",
			Nullable:           false,
			ToDataType:         "Float64",
			Result:             float64(42342.224),
			ToDataTypeInMomery: "float64",
		},
	}
	for _, v := range testCaseList {
		Convey(fmt.Sprintf("source %+v,toDataType:%s", v.Source, v.ToDataType), t, func() {
			result, err := CkDataTypeTransfer(v.Source, fieldName, v.ToDataType, v.Nullable)
			So(err, ShouldBeNil)
			So(result, ShouldNotBeNil)
			So(result, ShouldEqual, v.Result)
			So(reflect.TypeOf(result).String(), ShouldEqual, v.ToDataTypeInMomery)
		})
	}
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

	conn := &Conn{}
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
	sql = TransferNotes2Space(sql)
	t.Log(sql)
}

func TestTransferToCkTypeByColumnData(t *testing.T) {
	c := Conn{}
	type testCase struct {
		Source   interface{}
		Nullable bool
		Result   string
	}
	var testCaseList = []testCase{
		{
			Source:   nil,
			Nullable: true,
			Result:   "Nullable(String)",
		},
		{
			Source:   nil,
			Nullable: false,
			Result:   "String",
		},
		{
			Source:   int8(100),
			Nullable: false,
			Result:   "Int8",
		},
		{
			Source:   uint8(100),
			Nullable: false,
			Result:   "UInt8",
		},
		{
			Source:   int16(100),
			Nullable: false,
			Result:   "Int16",
		},
		{
			Source:   uint16(100),
			Nullable: false,
			Result:   "UInt16",
		},
		{
			Source:   int32(100),
			Nullable: false,
			Result:   "Int32",
		},
		{
			Source:   uint32(100),
			Nullable: false,
			Result:   "UInt32",
		},
		{
			Source:   int(100),
			Nullable: false,
			Result:   "Int64",
		},
		{
			Source:   uint(100),
			Nullable: false,
			Result:   "UInt64",
		},
		{
			Source:   int64(100),
			Nullable: false,
			Result:   "Int64",
		},
		{
			Source:   uint64(100),
			Nullable: false,
			Result:   "UInt64",
		},
		{
			Source:   float32(100.99),
			Nullable: false,
			Result:   "Float32",
		},
		{
			Source:   float64(100.99),
			Nullable: false,
			Result:   "Float64",
		},
		{
			Source:   []string{"aa"},
			Nullable: false,
			Result:   "String",
		},
		{
			Source:   map[string]interface{}{"a1": "a1_val"},
			Nullable: false,
			Result:   "String",
		},
		{
			Source:   interface{}("aaa"),
			Nullable: false,
			Result:   "String",
		},
		{
			Source:   "aaa",
			Nullable: false,
			Result:   "String",
		},
		{
			Source:   json.Number("1111"),
			Nullable: false,
			Result:   "String",
		},
		{
			Source:   "0000-00-00 00:00:00",
			Nullable: false,
			Result:   "DateTime",
		},
		{
			Source:   "2023-08-19 23:00:00",
			Nullable: false,
			Result:   "DateTime",
		},
		{
			Source:   "2023-08--9 23:00:00",
			Nullable: false,
			Result:   "String",
		},
		{
			Source:   "0000-00-00",
			Nullable: false,
			Result:   "Date",
		},
		{
			Source:   "2023-08-19",
			Nullable: false,
			Result:   "Date",
		},
		{
			Source:   "2023-0--19",
			Nullable: false,
			Result:   "String",
		},
		{
			Source:   "2023-08-19 23:00:00.123456",
			Nullable: false,
			Result:   "DateTime64(6)",
		},
		{
			Source:   "2023-08--9 23:00:00.123456",
			Nullable: false,
			Result:   "String",
		},
	}

	slice := make([]string, 0)
	slice = append(slice, "aa")
	slice = append(slice, "bb")
	testCaseList = append(testCaseList, testCase{
		Source:   slice,
		Nullable: false,
		Result:   "String",
	})

	Convey("test case", t, func() {
		for _, v := range testCaseList {
			toType := c.TransferToCkTypeByColumnData(v.Source, v.Nullable)
			SoMsg(fmt.Sprintf("%+v nullable:%+v", v.Source, v.Nullable), toType, ShouldEqual, v.Result)
		}
	})

}
