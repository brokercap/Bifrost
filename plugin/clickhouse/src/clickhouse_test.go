package src_test

import (
	"testing"
	"log"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	MyPlugin "github.com/brokercap/Bifrost/plugin/clickhouse/src"
	"time"
	"fmt"
	"reflect"
	"github.com/brokercap/Bifrost/sdk/pluginTestData"
	"strings"
	"strconv"
	"math"
	"database/sql/driver"
	"math/rand"
)

var url string = "tcp://192.168.126.140:9000?Database=test&debug=true&compress=1"
var engine  string = "MergeTree()"


//var createTable = "CREATE TABLE binlog_field_test(id UInt32,testtinyint Int8,testsmallint Int16,testmediumint Int32,testint Int32,testbigint Int64,testvarchar String,testchar String,testenum String,testset String,testtime String,testdate Date,testyear Int16,testtimestamp DateTime,testdatetime DateTime,testfloat Float64,testdouble Float64,testdecimal Float64,testtext String,testblob String,testbit Int64,testbool Int8,testmediumblob String,testlongblob String,testtinyblob String,test_unsinged_tinyint UInt8,test_unsinged_smallint UInt16,test_unsinged_mediumint UInt32,test_unsinged_int UInt32,test_unsinged_bigint UInt64,testjson String) ENGINE = MergeTree() ORDER BY (id);"
/*

CREATE TABLE binlog_field_test(id UInt32,testtinyint Int8,testsmallint Int16,testmediumint Int32,testint Int32,testbigint Int64,testvarchar String,testchar String,testenum String,testset String,testtime String,testdate Date,testyear Int16,testtimestamp DateTime,testdatetime DateTime,testfloat Float64,testdouble Float64,testdecimal Float64,testtext String,testblob String,testbit Int64,testbool Int8,testmediumblob String,testlongblob String,testtinyblob String,test_unsinged_tinyint UInt8,test_unsinged_smallint UInt16,test_unsinged_mediumint UInt32,test_unsinged_int UInt32,testjson String,test_unsinged_bigint UInt64) ENGINE = MergeTree() ORDER BY (id);
*/

var myConn MyPlugin.Conn
var conn pluginDriver.Driver
var event *pluginTestData.Event
var SchemaName = "bifrost_test"
var TableName = "binlog_field_test"

func testBefore(){
	conn = MyPlugin.NewConn()
	conn.SetOption(&url,nil)
	conn.Open()
	event = pluginTestData.NewEvent()
	event.SetSchema(SchemaName)
	event.SetTable(TableName)
}

func initDBTable(delTable bool) {
	c := MyPlugin.NewClickHouseDBConn(url)
	sql1:= "CREATE DATABASE IF NOT EXISTS  `"+SchemaName+"`"
	c.Exec(sql1,[]driver.Value{})
	sql2:="CREATE TABLE IF NOT EXISTS "+SchemaName+"."+TableName+"(id0 UInt32,id UInt32,testtinyint Int8,testsmallint Int16,testmediumint Int32,testint Int32,testbigint Int64,testvarchar String,testchar String,testenum String,testset String,testtime String,testdate Date,testyear Int16,testtimestamp DateTime,testdatetime DateTime,testfloat Float64,testdouble Float64,testdecimal Float64,testtext String,testblob String,testbit Int64,testbool Int8,testmediumblob String,testlongblob String,testtinyblob String,test_unsinged_tinyint UInt8,test_unsinged_smallint UInt16,test_unsinged_mediumint UInt32,test_unsinged_int UInt32,test_unsinged_bigint UInt64,bifrost_event_type String,testjson String,bifrost_data_version Int64) ENGINE = "+engine+" ORDER BY (id);"
	if delTable == false{
		c.Exec(sql2,[]driver.Value{})
	}else{
		sql3 := "DROP TABLE "+SchemaName+"."+TableName
		c.Exec(sql3,[]driver.Value{})
		err := c.Exec(sql2,[]driver.Value{})
		if err != nil{
			log.Fatal(err)
		}
		log.Println(sql2)
	}
	c.Close()
}

func initDBTablePriString(delTable bool) {
	c := MyPlugin.NewClickHouseDBConn(url)
	sql1:= "CREATE DATABASE IF NOT EXISTS  `"+SchemaName+"`"
	c.Exec(sql1,[]driver.Value{})
	sql2:="CREATE TABLE IF NOT EXISTS "+SchemaName+"."+TableName+"(id0 String,id String,testtinyint Int8,testsmallint Int16,testmediumint Int32,testint Int32,testbigint Int64,testvarchar String,testchar String,testenum String,testset String,testtime String,testdate Date,testyear Int16,testtimestamp DateTime,testdatetime DateTime,testfloat Float64,testdouble Float64,testdecimal Float64,testtext String,testblob String,testbit Int64,testbool Int8,testmediumblob String,testlongblob String,testtinyblob String,test_unsinged_tinyint UInt8,test_unsinged_smallint UInt16,test_unsinged_mediumint UInt32,test_unsinged_int UInt32,test_unsinged_bigint UInt64,bifrost_event_type String,testjson String,bifrost_data_version Int64) ENGINE = MergeTree() ORDER BY (id);"
	if delTable == false{
		c.Exec(sql2,[]driver.Value{})
	}else{
		sql3 := "DROP TABLE "+SchemaName+"."+TableName
		c.Exec(sql3,[]driver.Value{})
		c.Exec(sql2,[]driver.Value{})
	}
	c.Close()
}

func TestChechUri(t *testing.T){
	testBefore()
	if err := conn.CheckUri();err!= nil{
		t.Fatal("TestChechUri err:",err)
	}else{
		t.Log("TestChechUri success")
	}
}

func TestGetSchemaList(t *testing.T)  {
	c := MyPlugin.NewClickHouseDBConn(url)
	t.Log(c.GetSchemaList())
}


func TestGetSchemaTableList(t *testing.T)  {
	c := MyPlugin.NewClickHouseDBConn(url)
	t.Log(c.GetSchemaTableList("bifrost_test"))
}

func TestGetTableFields(t *testing.T)  {
	c := MyPlugin.NewClickHouseDBConn(url)
	t.Log(c.GetTableFields("bifrost_test","binlog_field_test_1"))
}

func getParam(args ...bool) map[string]interface{} {
	type fieldStruct struct {
		CK 		string
		MySQL 	string
	}

	param := make(map[string]interface{},0)
	Field := make([]fieldStruct,0)
	Field = append(Field,fieldStruct{"id0",""})
	Field = append(Field,fieldStruct{"id","id"})
	Field = append(Field,fieldStruct{"test_unsinged_bigint","test_unsinged_bigint"})
	Field = append(Field,fieldStruct{"test_unsinged_int","test_unsinged_int"})
	Field = append(Field,fieldStruct{"test_unsinged_mediumint","test_unsinged_mediumint"})
	Field = append(Field,fieldStruct{"test_unsinged_smallint","test_unsinged_smallint"})
	Field = append(Field,fieldStruct{"test_unsinged_tinyint","test_unsinged_tinyint"})
	Field = append(Field,fieldStruct{"testtinyint","testtinyint"})
	Field = append(Field,fieldStruct{"testsmallint","testsmallint"})
	Field = append(Field,fieldStruct{"testmediumint","testmediumint"})
	Field = append(Field,fieldStruct{"testint","testint"})
	Field = append(Field,fieldStruct{"testbigint","testbigint"})
	Field = append(Field,fieldStruct{"testbit","testbit"})
	Field = append(Field,fieldStruct{"testbool","testbool"})
	Field = append(Field,fieldStruct{"testvarchar","testvarchar"})
	Field = append(Field,fieldStruct{"testchar","testchar"})
	Field = append(Field,fieldStruct{"testtime","testtime"})
	Field = append(Field,fieldStruct{"testdate","testdate"})
	Field = append(Field,fieldStruct{"testyear","testyear"})

	Field = append(Field,fieldStruct{"testtimestamp","testtimestamp"})

	Field = append(Field,fieldStruct{"testdatetime","testdatetime"})
	Field = append(Field,fieldStruct{"testfloat","testfloat"})
	Field = append(Field,fieldStruct{"testdouble","testdouble"})
	Field = append(Field,fieldStruct{"testdecimal","testdecimal"})
	Field = append(Field,fieldStruct{"testtext","testtext"})
	Field = append(Field,fieldStruct{"testblob","testblob"})
	Field = append(Field,fieldStruct{"testmediumblob","testmediumblob"})
	Field = append(Field,fieldStruct{"testlongblob","testlongblob"})
	Field = append(Field,fieldStruct{"testtinyblob","testtinyblob"})
	Field = append(Field,fieldStruct{"testenum","testenum"})
	Field = append(Field,fieldStruct{"testset","testset"})
	Field = append(Field,fieldStruct{"bifrost_event_type","{$EventType}"})
	Field = append(Field,fieldStruct{"testjson","testjson"})
	Field = append(Field,fieldStruct{"bifrost_data_version","{$BifrostDataVersion}"})

	param["Field"] = Field

	PriKey := make([]fieldStruct,1)
	PriKey[0] = fieldStruct{"id","id"}
	param["PriKey"] = PriKey
	param["CkSchema"] = SchemaName
	param["CkTable"] = TableName
	param["BatchSize"] = 5000
	if len(args) > 0 {
		param["NullNotTransferDefault"] = args[0]
	}else{
		param["NullNotTransferDefault"] = false
	}
	return param
}

func getParamAutoCreateTable() map[string]interface{} {
	param := make(map[string]interface{},0)
	param["CkSchema"] = ""
	param["CkTable"] = ""
	param["BatchSize"] = 1000
	param["AutoCreateTable"] = true
	return param
}

func initSyncParam() {
	p,err := conn.SetParam(getParam())
	if err != nil{
		log.Println("set param fatal err")
		log.Fatal(err)
	}

	log.Println("p:",p)
}

func initSyncParamAutoCreateTable() {
	p,err := conn.SetParam(getParamAutoCreateTable())
	if err != nil{
		log.Println("set param fatal err")
		log.Fatal(err)
	}

	log.Println("p:",p)
}


func TestCommit(t *testing.T){
	testBefore()
	initDBTable(true)
	initSyncParam()
	insertdata := event.GetTestInsertData()
	conn.Insert(insertdata,false)
	conn.Del(event.GetTestDeleteData(),false)
	conn.Update(event.GetTestUpdateData(),false)

	conn.Insert(event.GetTestInsertData(),false)
	conn.Del(event.GetTestDeleteData(),false)
	conn.Insert(event.GetTestInsertData(),false)
	conn.Commit(event.GetTestCommitData(),false)
	_,_,err2 := conn.TimeOutCommit()
	if err2 != nil{
		t.Fatal(err2)
	}

	conn.Del(event.GetTestDeleteData(),false)
	conn.Update(event.GetTestUpdateData(),false)

	conn.Insert(event.GetTestInsertData(),false)
	conn.Del(event.GetTestDeleteData(),false)
	conn.Insert(event.GetTestInsertData(),false)
	conn.Insert(event.GetTestInsertData(),false)
	conn.Insert(event.GetTestInsertData(),false)
	conn.Commit(event.GetTestCommitData(),false)
	_,_,err2 = conn.TimeOutCommit()
	if err2 != nil{
		t.Fatal(err2)
	}
	t.Log("success")
}

func TestCommitPriKeyIsString(t *testing.T){
	testBefore()
	initDBTablePriString(true)
	initSyncParam()
	insertdata := event.GetTestInsertData()
	conn.Insert(insertdata,false)
	conn.Del(event.GetTestDeleteData(),false)
	conn.Update(event.GetTestUpdateData(),false)

	conn.Insert(event.GetTestInsertData(),false)
	conn.Del(event.GetTestDeleteData(),false)
	conn.Insert(event.GetTestInsertData(),false)
	_,_,err2 := conn.TimeOutCommit()
	if err2 != nil{
		t.Fatal(err2)
	}

	conn.Del(event.GetTestDeleteData(),false)
	conn.Update(event.GetTestUpdateData(),false)

	conn.Insert(event.GetTestInsertData(),false)
	conn.Del(event.GetTestDeleteData(),false)
	conn.Insert(event.GetTestInsertData(),false)
	conn.Insert(event.GetTestInsertData(),false)
	conn.Insert(event.GetTestInsertData(),false)
	_,_,err2 = conn.TimeOutCommit()
	if err2 != nil{
		t.Fatal(err2)
	}
	t.Log("success")
}

func TestReConnCommit(t *testing.T){
	testBefore()
	initDBTable(false)
	initSyncParam()
	conn.Insert(event.GetTestInsertData(),false)
	_,_,err1 := conn.TimeOutCommit()
	if err1 != nil{
		t.Fatal("err1",err1)
		return
	}else{
		t.Log("insert 1 success")
	}

	conn.Del(event.GetTestDeleteData(),false)
	conn.Update(event.GetTestUpdateData(),false)
	time.Sleep(20 * time.Second)
	for{
		time.Sleep(3 * time.Second)
		_,_,err2 := conn.TimeOutCommit()
		if err2 != nil{
			t.Error("err2:",err2)
		}else{
			break
		}
	}
	t.Log("success")
}

func TestInsertNullAndChekcData(t *testing.T){
	testBefore()

	initDBTable(true)
	initSyncParam()
	e := pluginTestData.NewEvent()
	e.SetIsNull(true)
	insertdata := e.GetTestInsertData()
	conn.Insert(insertdata,false)
	_,_,err2 := conn.TimeOutCommit()
	if err2 != nil{
		t.Fatal(err2)
	}

	c := MyPlugin.NewClickHouseDBConn(url)
	dataList := c.GetTableDataList(insertdata.SchemaName,insertdata.TableName,"id="+fmt.Sprint(insertdata.Rows[0]["id"]))

	for k,v := range dataList {
		t.Log("k:",k)
		for key,val := range v{
			t.Log(key,val)
		}
	}
	t.Log("success")
}

func TestCommitAndCheckData(t *testing.T){
	testBefore()
	initDBTable(true)
	initSyncParam()
	event := pluginTestData.NewEvent()
	eventData := event.GetTestInsertData()
	eventData = event.GetTestUpdateData()
	conn.Update(eventData,false)
	//conn.Insert(eventData)
	_,_,err2 := conn.TimeOutCommit()
	if err2 != nil{
		t.Fatal(err2)
	}

	m := eventData.Rows[len(eventData.Rows)-1]
	time.Sleep(1 * time.Second)
	c := MyPlugin.NewClickHouseDBConn(url)
	dataList := c.GetTableDataList(eventData.SchemaName,eventData.TableName,"id="+fmt.Sprint(m["id"]))

	if len(dataList) == 0{
		t.Fatal("select data len == 0")
	}

	resultData := make(map[string][]string,0)
	resultData["ok"] = make([]string,0)
	resultData["error"] = make([]string,0)

	checkDataRight(m,dataList[0],resultData)

	for _,v := range resultData["ok"] {
		t.Log(v)
	}

	for _,v := range resultData["error"] {
		t.Error(v)
	}

	if len(resultData["error"]) == 0{
		t.Log("test over;", "data is all right")
	}else{
		t.Error("test over;"," some data is error")
	}

}

func checkDataRight(m map[string]interface{},destMap map[string]driver.Value,resultData map[string][]string)  {
	for columnName,v := range destMap{
		if _,ok:=m[columnName];!ok{
			resultData["error"] = append(resultData["error"],fmt.Sprint(columnName," not exsit"))
		}
		var result bool = false
		switch m[columnName].(type) {
		case bool:
			if m[columnName].(bool) == true{
				if fmt.Sprint(v) == "1"{

					result = true
				}
			}else{
				if fmt.Sprint(v) == "0"{
					result = true
				}
			}
			break
		case []string:
			sourceData := strings.Replace(strings.Trim(fmt.Sprint(m[columnName]), "[]"), " ", ",", -1)
			if fmt.Sprint(v) == sourceData{
				result = true
			}
			break
		case float32,float64:
			//假如都是浮点数，因为精度问题，都先转成string 再转成 float64 ，再做差值处理，小于0.05 就算正常了
			floatDest,_ := strconv.ParseFloat(fmt.Sprint(v),64)
			floatSource,_ := strconv.ParseFloat(fmt.Sprint(m[columnName]),64)
			if math.Abs(floatDest - floatSource) < 0.05{
				result = true
			}
			break
			/*
			switch v.(type) {
			case float64,float32:
				floatDest,_ := strconv.ParseFloat(fmt.Sprint(v),64)
				floatSource,_ := strconv.ParseFloat(fmt.Sprint(m[columnName]),64)
				if math.Abs(floatDest - floatSource) < 0.05{
					result = true
				}
				break
			default :
				if fmt.Sprint(v) == fmt.Sprint(m[columnName]){
					result = true
				}
				break
			}
			break
			*/
		default:
			switch v.(type) {
			//这里需要去一次空格对比,因为有可能源是 带空格的字符串
			case int,uint,int8,uint8,int16,uint16,int32,uint32,int64,uint64,float32,float64:
				if strings.Trim(fmt.Sprint(v)," ") == strings.Trim(fmt.Sprint(m[columnName])," "){
					result = true
				}
				break
			case time.Time:
				// 这里用包括关系 ，也是因为 ck 读出来的时候，date和datetime类型都转成了time.Time 类型了
				descTime := fmt.Sprint(v.(time.Time).Format("2006-01-02 15:04:05"))
				if descTime == fmt.Sprint(m[columnName]) || strings.Index(descTime,fmt.Sprint(m[columnName])) == 0{
					result = true
				}
				break
			default:
				if fmt.Sprint(v) == fmt.Sprint(m[columnName]){
					result = true
				}
				break
			}

			break
		}
		if result{
			resultData["ok"] = append(resultData["ok"],fmt.Sprint(columnName," dest: ",v,"(",reflect.TypeOf(v),")"," == ",m[columnName],"(",reflect.TypeOf(m[columnName]),")"))
		}else{
			resultData["error"] = append(resultData["error"],fmt.Sprint(columnName," dest: ",v,"(",reflect.TypeOf(v),")"," != ",m[columnName],"(",reflect.TypeOf(m[columnName]),")"))
		}
	}
}


func TestRandDataAndCheck(t *testing.T){

	var n int = 1000

	e := pluginTestData.NewEvent()

	testBefore()
	initDBTable(true)

	initSyncParam()

	for i:=0;i<n;i++{
		var eventData *pluginDriver.PluginDataType
		rand.Seed(time.Now().UnixNano()+int64(i))
		switch rand.Intn(3){
		case 0:
			eventData = e.GetTestInsertData()
			conn.Insert(eventData,false)
			break
		case 1:
			eventData = e.GetTestUpdateData()
			conn.Update(eventData,false)
			break
		case 2:
			eventData = e.GetTestDeleteData()
			conn.Del(eventData,false)
			break
		case 3:
			eventData = e.GetTestQueryData()
			conn.Query(eventData,false)
			break
		}
	}
	conn.TimeOutCommit()

	resultData := make(map[string][]string,0)
	resultData["ok"] = make([]string,0)
	resultData["error"] = make([]string,0)

	c := MyPlugin.NewClickHouseDBConn(url)
	dataList := c.GetTableDataList(SchemaName,TableName,"")

	count := uint64(len(dataList))
	if count != uint64(len(e.GetDataMap())){
		for k,v := range e.GetDataMap(){
			t.Log(k ," ",v)
		}
		t.Fatal("ck Table Count:",count, " != srcDataCount:",len(e.GetDataMap()))
	}

	destMap := make(map[string]map[string]driver.Value,0)

	for _,v := range dataList{
		destMap[fmt.Sprint(v["id"])] = v
	}

	for _,data := range e.GetDataMap(){
		id:=fmt.Sprint(data["id"])
		checkDataRight(data,destMap[id],resultData)
	}

	for _,v := range resultData["ok"]{
		t.Log(v)
	}
	if len(resultData["error"]) > 0{
		for _,v := range resultData["error"]{
			t.Error(v)
		}
	}

	t.Log("ck Table Count:",count," srcDataCount:",len(e.GetDataMap()))

	t.Log("test over")
}



//模拟正式环境刷数据
func TestSyncLikeProduct(t *testing.T)  {
	initDBTable(true)
	p := pluginTestData.NewPlugin("clickhouse",url)
	err0 := p.SetParam(getParam())
	p.SetEventType(pluginTestData.INSERT)
	if err0 != nil{
		t.Fatal(err0)
	}

	var n uint = 10000
	err := p.DoTestStart(n)

	if err != nil{
		t.Fatal(err)
	}else{
		t.Log("test success")
	}
}

func TestCommitAndCheckData2(t *testing.T){
	testBefore()
	initDBTable(true)
	initSyncParam()
	event := pluginTestData.NewEvent()
	eventData := event.GetTestInsertData()
	eventData.Rows[0]["testint"] = "1334　"
	conn.Insert(eventData,false)
	_,_,err2 := conn.TimeOutCommit()
	if err2 != nil{
		t.Fatal(err2)
	}

	m := eventData.Rows[len(eventData.Rows)-1]
	time.Sleep(1 * time.Second)
	c := MyPlugin.NewClickHouseDBConn(url)
	dataList := c.GetTableDataList(eventData.SchemaName,eventData.TableName,"id="+fmt.Sprint(m["id"]))

	if len(dataList) == 0{
		t.Fatal("select data len == 0")
	}

	resultData := make(map[string][]string,0)
	resultData["ok"] = make([]string,0)
	resultData["error"] = make([]string,0)

	checkDataRight(m,dataList[0],resultData)

	for _,v := range resultData["ok"] {
		t.Log(v)
	}

	for _,v := range resultData["error"] {
		t.Error(v)
	}

	if len(resultData["error"]) == 0{
		t.Log("test over;", "data is all right")
	}else{
		t.Error("test over;"," some data is error")
	}

}



func TestConn_CkDataTypeTransfer(t *testing.T){
	var data int64 = 9223372036854775807
	var fieldName string
	var toDataType string
	fieldName = "testField"
	toDataType = "UInt8"
	t.Log("test start")
	result,err := MyPlugin.CkDataTypeTransfer(data,fieldName,toDataType,false)
	if err != nil{
		t.Fatal(err)
	}
	if reflect.TypeOf(result).String() == "uint8"{
		if result.(uint8) == uint8(0){
			t.Log("result(in8):",result)
		}else{
			t.Fatal("result:",result,"(",reflect.TypeOf(result),")")
		}
	}else{
		t.Fatal("result:",result,"(",reflect.TypeOf(result),")")
	}

	toDataType = "UInt8"
	result,err = MyPlugin.CkDataTypeTransfer(data,fieldName,toDataType,false)
	if err != nil{
		t.Fatal(err)
	}

	if reflect.TypeOf(result).String() == "uint8"{
		if result.(uint8) == uint8(0){
			t.Log("result(uint8):",result)
		}else{
			t.Fatal("result:",result,"(",reflect.TypeOf(result),")")
		}
	}else{
		t.Fatal("result:",result,"(",reflect.TypeOf(result),")")
	}


	toDataType = "Int16"
	result,err = MyPlugin.CkDataTypeTransfer(data,fieldName,toDataType,false)
	if err != nil{
		t.Fatal(err)
	}

	if reflect.TypeOf(result).String() == "int16"{
		if result.(int16) == int16(0){
			t.Log("result(int16):",result)
		}else{
			t.Fatal("result:",result,"(",reflect.TypeOf(result),")")
		}
	}else{
		t.Fatal("result:",result,"(",reflect.TypeOf(result),")")
	}



	toDataType = "UInt16"
	result,err = MyPlugin.CkDataTypeTransfer(data,fieldName,toDataType,false)
	if err != nil{
		t.Fatal(err)
	}

	if reflect.TypeOf(result).String() == "uint16"{
		if result.(uint16) == uint16(0){
			t.Log("result(uint16):",result)
		}else{
			t.Fatal("result:",result,"(",reflect.TypeOf(result),")")
		}
	}else{
		t.Fatal("result:",result,"(",reflect.TypeOf(result),")")
	}


	toDataType = "Int32"
	result,err = MyPlugin.CkDataTypeTransfer(data,fieldName,toDataType,false)
	if err != nil{
		t.Fatal(err)
	}

	if reflect.TypeOf(result).String() == "int32"{
		if result.(int32) == int32(0){
			t.Log("result(int32):",result)
		}else{
			t.Fatal("result:",result,"(",reflect.TypeOf(result),")")
		}
	}else{
		t.Fatal("result:",result,"(",reflect.TypeOf(result),")")
	}



	toDataType = "UInt32"
	result,err = MyPlugin.CkDataTypeTransfer(data,fieldName,toDataType,false)
	if err != nil{
		t.Fatal(err)
	}

	if reflect.TypeOf(result).String() == "uint32"{
		if result.(uint32) == uint32(0){
			t.Log("result(uint32):",result)
		}else{
			t.Fatal("result:",result,"(",reflect.TypeOf(result),")")
		}
	}else{
		t.Fatal("result:",result,"(",reflect.TypeOf(result),")")
	}

}

func TestConn_AutoCreateTableCommit(t *testing.T) {
	TableName = "mytest"
	testBefore()
	initSyncParamAutoCreateTable()
	event := pluginTestData.NewEvent()
	eventData := event.GetTestInsertData()
	conn.Insert(eventData,false)
	_,_,err2 := conn.TimeOutCommit()
	if err2 != nil{
		t.Fatal(err2)
	}
}

func initDBTableDefaultNullVal(delTable bool) {
	c := MyPlugin.NewClickHouseDBConn(url)
	sql1:= "CREATE DATABASE IF NOT EXISTS  `"+SchemaName+"`"
	c.Exec(sql1,[]driver.Value{})
	engine=" ReplacingMergeTree(id)"
	sql2:="CREATE TABLE IF NOT EXISTS "+SchemaName+"."+TableName+"(id UInt32,testtinyint Nullable(Int8),testsmallint Nullable(Int16),testmediumint Nullable(Int32),testint Nullable(Int32),testbigint Nullable(Int64),testvarchar Nullable(String),testchar Nullable(String),test_unsinged_tinyint Nullable(UInt8),test_unsinged_smallint Nullable(UInt16),test_unsinged_mediumint Nullable(UInt32),test_unsinged_int Nullable(UInt32),test_unsinged_bigint Nullable(UInt64),bifrost_event_type String,testjson String,bifrost_data_version Int64) ENGINE = "+engine+" ORDER BY (id);"
	if delTable == false{
		c.Exec(sql2,[]driver.Value{})
	}else{
		sql3 := "DROP TABLE "+SchemaName+"."+TableName
		c.Exec(sql3,[]driver.Value{})
		err := c.Exec(sql2,[]driver.Value{})
		if err != nil{
			log.Fatal(err)
		}
		log.Println(sql2)
	}
	c.Close()
}

func TestConn_NullNotTransferDefault_CommitAndCheck(t *testing.T)  {
	TableName = "write_nil_test"
	testBefore()
	event.SetIsNull(true)
	event.SetTable(TableName)
	initDBTableDefaultNullVal(true)
	conn.SetParam(getParam(true))
	eventData := event.GetTestInsertData()
	conn.Insert(eventData,false)
	_,_,err := conn.TimeOutCommit()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(eventData)
	time.Sleep(1 * time.Second)
	c := MyPlugin.NewClickHouseDBConn(url)
	dataList := c.GetTableDataList(eventData.SchemaName,eventData.TableName,"id="+fmt.Sprint(eventData.Rows[0]["id"]))
	if len(dataList) == 0{
		t.Fatal("select data len == 0")
	}

	for k,v := range dataList[0]{
		if k == "id"{
			if fmt.Sprint(v) != fmt.Sprint(eventData.Rows[0]["id"]){
				t.Fatal("id: ",eventData.Rows[0]["id"] ," != ",v ,"(must)")
			}
			continue
		}
		if k == "bifrost_event_type" || k == "bifrost_data_version" {
			continue
		}
		// testjson 设置是 String ，不是Nullable(String)，
		if k == "testjson" {
			if v.(string) != "" {
				t.Fatal("testjson: "," !=","''","(must)")
			}
			continue
		}
		if v != nil {
			t.Fatal(k, "!= nil", " data:",v)
		}
	}

	t.Log("success")

}

func TestNewTableData(t *testing.T) {
	c := MyPlugin.NewTableData()
	if c.CommitData[0] == nil {
		t.Log("test frist 0 index is nil")
	}
	c.CommitData = c.CommitData[1:]
	t.Log("success")
}
