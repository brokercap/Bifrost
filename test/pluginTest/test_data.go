package pluginTest

import (
	"encoding/json"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"time"
)

var TestInsertDataJson = `[{"id":1,"test_unsinged_bigint":5,"test_unsinged_int":4,"test_unsinged_mediumint":3,"test_unsinged_smallint":2,"test_unsinged_tinyint":1,"testbigint":-5,"testbit":8,"testblob":"testblob","testbool":true,"testchar":"te","testdate":"2018-05-08","testdatetime":"2018-05-08 15:30:21","testdecimal":"9.39","testdouble":9.39,"testenum":"en2","testfloat":9.39,"testint":-4,"testlongblob":"testlongblob","testmediumblob":"testmediumblob","testmediumint":-3,"testset":["set1","set3"],"testsmallint":-2,"testtext":"testtext","testtime":"15:39:59","testtimestamp":"2018-05-08 15:30:21","testtinyblob":"testtinyblob","testtinyint":-1,"testvarchar":"testvarcha","testyear":"2018"}]`
var TestUpdateDataJson = `[{"id":1,"test_unsinged_bigint":5,"test_unsinged_int":4,"test_unsinged_mediumint":3,"test_unsinged_smallint":2,"test_unsinged_tinyint":1,"testbigint":-5,"testbit":8,"testblob":"testblob","testbool":true,"testchar":"te","testdate":"2018-05-08","testdatetime":"2018-05-08 15:30:21","testdecimal":"9.39","testdouble":9.39,"testenum":"en2","testfloat":9.39,"testint":-4,"testlongblob":"testlongblob","testmediumblob":"testmediumblob","testmediumint":-3,"testset":["set1","set3"],"testsmallint":-2,"testtext":"testtext","testtime":"15:39:59","testtimestamp":"2018-05-08 15:30:21","testtinyblob":"testtinyblob","testtinyint":-1,"testvarchar":"testvarcha","testyear":"2018"},{"id":1,"test_unsinged_bigint":5,"test_unsinged_int":4,"test_unsinged_mediumint":3,"test_unsinged_smallint":2,"test_unsinged_tinyint":1,"testbigint":-5,"testbit":10,"testblob":"testblob","testbool":true,"testchar":"te","testdate":"2018-05-08","testdatetime":"2018-05-08 15:30:21","testdecimal":"9.39","testdouble":9.39,"testenum":"en2","testfloat":9.39,"testint":-4,"testlongblob":"testlongblob","testmediumblob":"testmediumblob","testmediumint":-3,"testset":["set1","set3"],"testsmallint":-2,"testtext":"testtext","testtime":"15:39:59","testtimestamp":"2018-05-08 15:30:21","testtinyblob":"testtinyblob","testtinyint":-1,"testvarchar":"mytest","testyear":"2018"}]`
var TestDeleteDataJson = `[{"id":1,"test_unsinged_bigint":5,"test_unsinged_int":4,"test_unsinged_mediumint":3,"test_unsinged_smallint":2,"test_unsinged_tinyint":1,"testbigint":-5,"testbit":10,"testblob":"testblob","testbool":true,"testchar":"te","testdate":"2018-05-08","testdatetime":"2018-05-08 15:30:21","testdecimal":"9.39","testdouble":9.39,"testenum":"en2","testfloat":9.39,"testint":-4,"testlongblob":"testlongblob","testmediumblob":"testmediumblob","testmediumint":-3,"testset":["set1","set3"],"testsmallint":-2,"testtext":"testtext","testtime":"15:39:59","testtimestamp":"2018-05-08 15:30:21","testtinyblob":"testtinyblob","testtinyint":-1,"testvarchar":"mytest","testyear":"2018"}]`
var TestQueryData = "ALTER TABLE `bifrost_test`.`binlog_field_http_plugin_test` CHANGE COLUMN `testvarchar` `testvarchar` varchar(20) NOT NULL"

type dataStruct struct {
	Id                      uint32 `json:"id"`
	Test_unsinged_bigint    uint64 `json:"test_unsinged_bigint"`
	Test_unsinged_int       uint32 `json:"test_unsinged_int"`
	Test_unsinged_mediumint uint32 `json:"test_unsinged_mediumint"`
	Test_unsinged_smallint  uint16 `json:"test_unsinged_smallint"`
	Test_unsinged_tinyint   uint8  `json:"test_unsinged_tinyint"`

	Testtinyint   int8  `json:"testtinyint"`
	Testsmallint  int16 `json:"testsmallint"`
	Testmediumint int32 `json:"testmediumint"`
	Testint       int32 `json:"testint"`
	Testbigint    int64 `json:"testbigint"`

	Testbit  int64 `json:"testbit"`
	Testbool bool  `json:"testbool"`

	Testvarchar string `json:"testvarchar"`
	Testchar    string `json:"testchar"`

	Testtime      string `json:"testtime"`
	Testdate      string `json:"testdate"`
	Testyear      string `json:"testyear"`
	Testtimestamp string `json:"testtimestamp"`
	Testdatetime  string `json:"testdatetime"`

	Testfloat   float64 `json:"testfloat"`
	Testdouble  float64 `json:"testdouble"`
	Testdecimal string  `json:"testdecimal"`

	Testtext       string `json:"testtext"`
	Testblob       string `json:"testblob"`
	Testmediumblob string `json:"testmediumblob"`
	Testlongblob   string `json:"testlongblob"`
	Testtinyblob   string `json:"testtinyblob"`

	Testenum string   `json:"testenum"`
	Testset  []string `json:"testset"`
}

func GetTestInsertData() *pluginDriver.PluginDataType {
	var Rows []map[string]interface{}

	var data []dataStruct
	json.Unmarshal([]byte(TestInsertDataJson), &data)

	Rows = make([]map[string]interface{}, 1)

	m := make(map[string]interface{}, 0)
	m["id"] = data[0].Id
	m["test_unsinged_bigint"] = data[0].Test_unsinged_bigint
	m["test_unsinged_int"] = data[0].Test_unsinged_int
	m["test_unsinged_mediumint"] = data[0].Test_unsinged_mediumint
	m["test_unsinged_smallint"] = data[0].Test_unsinged_smallint
	m["test_unsinged_tinyint"] = data[0].Test_unsinged_tinyint
	m["testtinyint"] = data[0].Testtinyint
	m["testsmallint"] = data[0].Testsmallint
	m["testmediumint"] = data[0].Testmediumint
	m["testint"] = data[0].Testint
	m["testbigint"] = data[0].Testbigint
	m["testbit"] = data[0].Testbit
	m["testbool"] = data[0].Testbool
	m["testvarchar"] = data[0].Testvarchar
	m["testchar"] = data[0].Testchar
	m["testtime"] = data[0].Testtime
	m["testyear"] = data[0].Testyear
	m["testdate"] = data[0].Testdate

	m["testtimestamp"] = data[0].Testtimestamp
	m["testdatetime"] = data[0].Testdatetime

	m["testfloat"] = data[0].Testfloat
	m["testdouble"] = data[0].Testdouble
	m["testdecimal"] = data[0].Testdecimal

	m["testtext"] = data[0].Testtext
	m["testblob"] = data[0].Testblob
	m["testmediumblob"] = data[0].Testmediumblob
	m["testlongblob"] = data[0].Testlongblob
	m["testtinyblob"] = data[0].Testtinyblob
	m["testenum"] = data[0].Testenum
	m["testset"] = data[0].Testset

	Rows[0] = m

	return &pluginDriver.PluginDataType{
		Timestamp:      uint32(time.Now().Unix()),
		EventType:      "insert",
		Rows:           Rows,
		Query:          "",
		SchemaName:     "bifrost_test",
		TableName:      "bifrost_field_test",
		BinlogFileNum:  10,
		BinlogPosition: 100,
	}
}

func GetTestUpdateData() *pluginDriver.PluginDataType {
	var Rows []map[string]interface{}

	var data []dataStruct
	json.Unmarshal([]byte(TestUpdateDataJson), &data)

	Rows = make([]map[string]interface{}, 2)

	m := make(map[string]interface{}, 0)
	m["id"] = data[0].Id
	m["test_unsinged_bigint"] = data[0].Test_unsinged_bigint
	m["test_unsinged_int"] = data[0].Test_unsinged_int
	m["test_unsinged_mediumint"] = data[0].Test_unsinged_mediumint
	m["test_unsinged_smallint"] = data[0].Test_unsinged_smallint
	m["test_unsinged_tinyint"] = data[0].Test_unsinged_tinyint
	m["testtinyint"] = data[0].Testtinyint
	m["testsmallint"] = data[0].Testsmallint
	m["testmediumint"] = data[0].Testmediumint
	m["testint"] = data[0].Testint
	m["testbigint"] = data[0].Testbigint
	m["testbit"] = data[0].Testbit
	m["testbool"] = data[0].Testbool
	m["testvarchar"] = data[0].Testvarchar
	m["testchar"] = data[0].Testchar
	m["testtime"] = data[0].Testtime
	m["testyear"] = data[0].Testyear
	m["testdate"] = data[0].Testdate
	m["testtimestamp"] = data[0].Testtimestamp
	m["testdatetime"] = data[0].Testdatetime

	m["testfloat"] = data[0].Testfloat
	m["testdouble"] = data[0].Testdouble
	m["testdecimal"] = data[0].Testdecimal

	m["testtext"] = data[0].Testtext
	m["testblob"] = data[0].Testblob
	m["testmediumblob"] = data[0].Testmediumblob
	m["testlongblob"] = data[0].Testlongblob
	m["testtinyblob"] = data[0].Testtinyblob
	m["testenum"] = data[0].Testenum
	m["testset"] = data[0].Testset

	m1 := make(map[string]interface{}, 0)

	m1["id"] = data[1].Id
	m1["test_unsinged_bigint"] = data[1].Test_unsinged_bigint
	m1["test_unsinged_int"] = data[1].Test_unsinged_int
	m1["test_unsinged_mediumint"] = data[1].Test_unsinged_mediumint
	m1["test_unsinged_smallint"] = data[1].Test_unsinged_smallint
	m1["test_unsinged_tinyint"] = data[1].Test_unsinged_tinyint
	m1["testtinyint"] = data[1].Testtinyint
	m1["testsmallint"] = data[1].Testsmallint
	m1["testmediumint"] = data[1].Testmediumint
	m1["testint"] = data[1].Testint
	m1["testbigint"] = data[1].Testbigint
	m1["testbit"] = data[1].Testbit
	m1["testbool"] = data[1].Testbool
	m1["testvarchar"] = data[1].Testvarchar
	m1["testchar"] = data[1].Testchar
	m1["testtime"] = data[1].Testtime
	m1["testyear"] = data[1].Testyear
	m1["testdate"] = data[1].Testdate
	m1["testtimestamp"] = data[1].Testtimestamp
	m1["testdatetime"] = data[1].Testdatetime

	m1["testfloat"] = data[1].Testfloat
	m1["testdouble"] = data[1].Testdouble
	m1["testdecimal"] = data[1].Testdecimal

	m1["testtext"] = data[1].Testtext
	m1["testblob"] = data[1].Testblob
	m1["testmediumblob"] = data[1].Testmediumblob
	m1["testlongblob"] = data[1].Testlongblob
	m1["testtinyblob"] = data[1].Testtinyblob
	m1["testenum"] = data[1].Testenum
	m1["testset"] = data[1].Testset

	Rows[0] = m
	Rows[1] = m1

	return &pluginDriver.PluginDataType{
		Timestamp:      uint32(time.Now().Unix()),
		EventType:      "update",
		Rows:           Rows,
		Query:          "",
		SchemaName:     "bifrost_test",
		TableName:      "bifrost_field_test",
		BinlogFileNum:  10,
		BinlogPosition: 100,
	}
}

func GetTestDeleteData() *pluginDriver.PluginDataType {
	var Rows []map[string]interface{}

	var data []dataStruct
	json.Unmarshal([]byte(TestDeleteDataJson), &data)

	Rows = make([]map[string]interface{}, 1)

	m := make(map[string]interface{}, 0)
	m["id"] = data[0].Id
	m["test_unsinged_bigint"] = data[0].Test_unsinged_bigint
	m["test_unsinged_int"] = data[0].Test_unsinged_int
	m["test_unsinged_mediumint"] = data[0].Test_unsinged_mediumint
	m["test_unsinged_smallint"] = data[0].Test_unsinged_smallint
	m["test_unsinged_tinyint"] = data[0].Test_unsinged_tinyint
	m["testtinyint"] = data[0].Testtinyint
	m["testsmallint"] = data[0].Testsmallint
	m["testmediumint"] = data[0].Testmediumint
	m["testint"] = data[0].Testint
	m["testbigint"] = data[0].Testbigint
	m["testbit"] = data[0].Testbit
	m["testbool"] = data[0].Testbool
	m["testvarchar"] = data[0].Testvarchar
	m["testchar"] = data[0].Testchar
	m["testtime"] = data[0].Testtime
	m["testyear"] = data[0].Testyear
	m["testdate"] = data[0].Testdate
	m["testtimestamp"] = data[0].Testtimestamp
	m["testdatetime"] = data[0].Testdatetime

	m["testfloat"] = data[0].Testfloat
	m["testdouble"] = data[0].Testdouble
	m["testdecimal"] = data[0].Testdecimal

	m["testtext"] = data[0].Testtext
	m["testblob"] = data[0].Testblob
	m["testmediumblob"] = data[0].Testmediumblob
	m["testlongblob"] = data[0].Testlongblob
	m["testtinyblob"] = data[0].Testtinyblob
	m["testenum"] = data[0].Testenum
	m["testset"] = data[0].Testset

	Rows[0] = m
	return &pluginDriver.PluginDataType{
		Timestamp:      uint32(time.Now().Unix()),
		EventType:      "delete",
		Rows:           Rows,
		Query:          "",
		SchemaName:     "bifrost_test",
		TableName:      "bifrost_field_test",
		BinlogFileNum:  10,
		BinlogPosition: 100,
	}
}

func GetTestQueryData() *pluginDriver.PluginDataType {
	var Rows []map[string]interface{}
	Rows = make([]map[string]interface{}, 0)
	return &pluginDriver.PluginDataType{
		Timestamp:      uint32(time.Now().Unix()),
		EventType:      "sql",
		Rows:           Rows,
		Query:          TestQueryData,
		SchemaName:     "bifrost_test",
		TableName:      "bifrost_field_test",
		BinlogFileNum:  10,
		BinlogPosition: 100,
	}
}
