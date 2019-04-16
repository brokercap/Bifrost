package pluginTest

import (
	pluginDriver "github.com/jc3wish/Bifrost/plugin/driver"
	"time"
	"encoding/json"
)

var TestInsertDataJson = `[{"id":1,"test_unsinged_bigint":5,"test_unsinged_int":4,"test_unsinged_mediumint":3,"test_unsinged_smallint":2,"test_unsinged_tinyint":1,"testbigint":-5,"testbit":8,"testblob":"testblob","testbool":true,"testchar":"te","testdate":"2018-05-08","testdatetime":"2018-05-08 15:30:21","testdecimal":"9.39","testdouble":9.39,"testenum":"en2","testfloat":9.39,"testint":-4,"testlongblob":"testlongblob","testmediumblob":"testmediumblob","testmediumint":-3,"testset":["set1","set3"],"testsmallint":-2,"testtext":"testtext","testtime":"15:39:59","testtimestamp":"2018-05-08 15:30:21","testtinyblob":"testtinyblob","testtinyint":-1,"testvarchar":"testvarcha","testyear":"2018"}]`
var TestUpdateDataJson = `[{"id":1,"test_unsinged_bigint":5,"test_unsinged_int":4,"test_unsinged_mediumint":3,"test_unsinged_smallint":2,"test_unsinged_tinyint":1,"testbigint":-5,"testbit":8,"testblob":"testblob","testbool":true,"testchar":"te","testdate":"2018-05-08","testdatetime":"2018-05-08 15:30:21","testdecimal":"9.39","testdouble":9.39,"testenum":"en2","testfloat":9.39,"testint":-4,"testlongblob":"testlongblob","testmediumblob":"testmediumblob","testmediumint":-3,"testset":["set1","set3"],"testsmallint":-2,"testtext":"testtext","testtime":"15:39:59","testtimestamp":"2018-05-08 15:30:21","testtinyblob":"testtinyblob","testtinyint":-1,"testvarchar":"testvarcha","testyear":"2018"},{"id":1,"test_unsinged_bigint":5,"test_unsinged_int":4,"test_unsinged_mediumint":3,"test_unsinged_smallint":2,"test_unsinged_tinyint":1,"testbigint":-5,"testbit":10,"testblob":"testblob","testbool":true,"testchar":"te","testdate":"2018-05-08","testdatetime":"2018-05-08 15:30:21","testdecimal":"9.39","testdouble":9.39,"testenum":"en2","testfloat":9.39,"testint":-4,"testlongblob":"testlongblob","testmediumblob":"testmediumblob","testmediumint":-3,"testset":["set1","set3"],"testsmallint":-2,"testtext":"testtext","testtime":"15:39:59","testtimestamp":"2018-05-08 15:30:21","testtinyblob":"testtinyblob","testtinyint":-1,"testvarchar":"mytest","testyear":"2018"}]`
var TestDeleteDataJson = `[{"id":1,"test_unsinged_bigint":5,"test_unsinged_int":4,"test_unsinged_mediumint":3,"test_unsinged_smallint":2,"test_unsinged_tinyint":1,"testbigint":-5,"testbit":10,"testblob":"testblob","testbool":true,"testchar":"te","testdate":"2018-05-08","testdatetime":"2018-05-08 15:30:21","testdecimal":"9.39","testdouble":9.39,"testenum":"en2","testfloat":9.39,"testint":-4,"testlongblob":"testlongblob","testmediumblob":"testmediumblob","testmediumint":-3,"testset":["set1","set3"],"testsmallint":-2,"testtext":"testtext","testtime":"15:39:59","testtimestamp":"2018-05-08 15:30:21","testtinyblob":"testtinyblob","testtinyint":-1,"testvarchar":"mytest","testyear":"2018"}]`
var TestQueryData = "ALTER TABLE `bifrost_test`.`binlog_field_http_plugin_test` CHANGE COLUMN `testvarchar` `testvarchar` varchar(20) NOT NULL"

func GetTestInsertData() *pluginDriver.PluginDataType{
	var Rows []map[string]interface{}
	json.Unmarshal([]byte(TestInsertDataJson),&Rows)
	return &pluginDriver.PluginDataType{
		Timestamp 		: uint32(time.Now().Unix()),
		EventType 		: "insert",
		Rows            : Rows,
		Query          	: "",
		SchemaName     	: "bifrost_test",
		TableName      	: "bifrost_field_test",
		BinlogFileNum 	: 10,
		BinlogPosition 	: 100,
	}
}

func GetTestUpdateData() *pluginDriver.PluginDataType{
	var Rows []map[string]interface{}
	json.Unmarshal([]byte(TestUpdateDataJson),&Rows)
	return &pluginDriver.PluginDataType{
		Timestamp 		: uint32(time.Now().Unix()),
		EventType 		: "insert",
		Rows            : Rows,
		Query          	: "",
		SchemaName     	: "bifrost_test",
		TableName      	: "bifrost_field_test",
		BinlogFileNum 	: 10,
		BinlogPosition 	: 100,
	}
}


func GetTestDeleteData() *pluginDriver.PluginDataType{
	var Rows []map[string]interface{}
	json.Unmarshal([]byte(TestDeleteDataJson),&Rows)
	return &pluginDriver.PluginDataType{
		Timestamp 		: uint32(time.Now().Unix()),
		EventType 		: "insert",
		Rows            : Rows,
		Query          	: "",
		SchemaName     	: "bifrost_test",
		TableName      	: "bifrost_field_test",
		BinlogFileNum 	: 10,
		BinlogPosition 	: 100,
	}
}

func GetTestQueryData() *pluginDriver.PluginDataType{
	var Rows []map[string]interface{}
	Rows = make([]map[string]interface{},0)
	return &pluginDriver.PluginDataType{
		Timestamp 		: uint32(time.Now().Unix()),
		EventType 		: "insert",
		Rows            : Rows,
		Query          	: TestQueryData,
		SchemaName     	: "bifrost_test",
		TableName      	: "bifrost_field_test",
		BinlogFileNum 	: 10,
		BinlogPosition 	: 100,
	}
}

