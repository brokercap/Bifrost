package src_test

import (
	"testing"
	"log"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"github.com/brokercap/Bifrost/test/pluginTest"
	"github.com/brokercap/Bifrost/sdk/pluginTestData"
	MyPlugin "github.com/brokercap/Bifrost/plugin/mysql/src"
	dbDriver "database/sql/driver"
	"github.com/brokercap/Bifrost/Bristol/mysql"
	"strings"
	"github.com/brokercap/Bifrost/util/dataType"
	"github.com/brokercap/Bifrost/server/history"
	"reflect"
	"fmt"
)

var url string = "root:root123@tcp(10.40.6.89:3306)/bifrost_test"

var SchemaName string = "bifrost_test"
var TableName string = "binlog_field_test"
/*
ddl

CREATE TABLE binlog_field_test(id UInt32,testtinyint Int8,testsmallint Int16,testmediumint Int32,testint Int32,testbigint Int64,testvarchar String,testchar String,testenum String,testset String,testtime String,testdate Date,testyear Int16,testtimestamp DateTime,testdatetime DateTime,testfloat Float64,testdouble Float64,testdecimal Float64,testtext String,testblob String,testbit Int64,testbool Int8,testmediumblob String,testlongblob String,testtinyblob String,test_unsinged_tinyint UInt8,test_unsinged_smallint UInt16,test_unsinged_mediumint UInt32,test_unsinged_int UInt32,test_unsinged_bigint UInt64) ENGINE = MergeTree() ORDER BY (id);
 */

func TestChechUri(t *testing.T){
	myConn := MyPlugin.MyConn{}
	if err := myConn.CheckUri(url);err!= nil{
		log.Println("TestChechUri err:",err)
	}else{
		log.Println("TestChechUri success")
	}
}

func TestGetSchemaList(t *testing.T)  {
	c := MyPlugin.NewMysqlDBConn(url)
	log.Println(c.GetSchemaList())
}


func TestGetSchemaTableList(t *testing.T)  {
	c := MyPlugin.NewMysqlDBConn(url)
	log.Println(c.GetSchemaTableList("test"))
}

func TestGetTableFields(t *testing.T)  {
	c := MyPlugin.NewMysqlDBConn(url)
	log.Println(c.GetTableFields(SchemaName,TableName))
}

func beforeTest()  {
	
}

func initDBTable(delTable bool) {
	c := mysql.NewConnect(url)
	sql1:= "CREATE DATABASE IF NOT EXISTS  `"+SchemaName+"`";
	c.Exec(sql1,[]dbDriver.Value{})
	sql2:="CREATE TABLE IF NOT EXISTS "+SchemaName+"."+TableName+"(id UInt32,testtinyint Int8,testsmallint Int16,testmediumint Int32,testint Int32,testbigint Int64,testvarchar String,testchar String,testenum String,testset String,testtime String,testdate Date,testyear Int16,testtimestamp DateTime,testdatetime DateTime,testfloat Float64,testdouble Float64,testdecimal Float64,testtext String,testblob String,testbit Int64,testbool Int8,testmediumblob String,testlongblob String,testtinyblob String,test_unsinged_tinyint UInt8,test_unsinged_smallint UInt16,test_unsinged_mediumint UInt32,test_unsinged_int UInt32,test_unsinged_bigint UInt64) ENGINE = MergeTree() ORDER BY (id);"
	if delTable == false{
		c.Exec(sql2,[]dbDriver.Value{})
	}else{
		sql3 := "DROP TABLE "+SchemaName+"."+TableName;
		c.Exec(sql3,[]dbDriver.Value{})
		c.Exec(sql2,[]dbDriver.Value{})
	}
	c.Close()
}


func getParam()  map[string]interface{}{
	type fieldStruct struct {
		ToField 		string
		FromMysqlField 	string
	}

	param := make(map[string]interface{},0)
	Field := make([]fieldStruct,0)
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

	sql := ""
	for _,f := range Field{
		sql += f.ToField+","
	}
	log.Println(sql)
	param["Field"] = Field

	PriKey := make([]fieldStruct,1)
	PriKey[0] = fieldStruct{"id","id"}
	param["PriKey"] = PriKey
	param["Schema"] = SchemaName
	param["Table"] = TableName

	return param
}

func getPluginConn() pluginDriver.ConnFun {
	myConn := MyPlugin.MyConn{}
	conn := myConn.Open(url)

	p,err := conn.SetParam(getParam())
	if err != nil{
		log.Println("set param fatal err")
		log.Fatal(err)
	}

	log.Println("p:",p)
	return conn
}


func TestCommit(t *testing.T){
	conn := getPluginConn()
	insertdata := pluginTest.GetTestInsertData()
	log.Println("testtimestamp",insertdata.Rows[0]["testtimestamp"])
	conn.Insert(insertdata)
	conn.Del(pluginTest.GetTestDeleteData())
	conn.Update(pluginTest.GetTestUpdateData())
	_,err2 := conn.Commit()
	if err2 != nil{
		log.Fatal(err2)
	}
}


func TestInsertAndChekcData(t *testing.T){
	beforeTest()
	initDBTable(false)
	conn := getPluginConn()
	e := pluginTestData.NewEvent()
	insertdata := e.GetTestInsertData()
	conn.Insert(insertdata)
	_,err2 := conn.Commit()
	if err2 != nil{
		t.Fatal(err2)
	}

	checkResult,err := checkDataRight(insertdata.Rows[len(insertdata.Rows)-1])
	if err != nil{
		t.Fatal(err)
	}

	for _,v := range checkResult["ok"]{
		t.Log(v)
	}

	for _,v := range checkResult["error"]{
		t.Error(v)
	}
}

func TestUpdateAndChekcData(t *testing.T){
	beforeTest()
	initDBTable(false)
	conn := getPluginConn()
	e := pluginTestData.NewEvent()
	insertdata := e.GetTestInsertData()
	conn.Insert(insertdata)

	updateData := e.GetTestUpdateData()
	conn.Update(updateData)
	_,err2 := conn.Commit()
	if err2 != nil{
		t.Fatal(err2)
	}

	checkResult,err := checkDataRight(updateData.Rows[len(updateData.Rows)-1])
	if err != nil{
		t.Fatal(err)
	}

	for _,v := range checkResult["ok"]{
		t.Log(v)
	}

	for _,v := range checkResult["error"]{
		t.Error(v)
	}
}


func TestDelAndChekcData(t *testing.T){
	beforeTest()
	initDBTable(false)
	conn := getPluginConn()
	e := pluginTestData.NewEvent()
	insertdata := e.GetTestInsertData()
	conn.Insert(insertdata)

	updateData := e.GetTestUpdateData()
	conn.Update(updateData)

	deleteData := e.GetTestDeleteData()
	conn.Del(deleteData)
	_,err2 := conn.Commit()
	if err2 != nil{
		t.Fatal(err2)
	}
	m,err:=getMysqlData(fmt.Sprint(deleteData.Rows[len(deleteData.Rows)-1]["id"]))
	if err != nil{
		t.Fatal(err)
	}

	if len(m) == 0{
		t.Log("test delete success")
	}else{
		t.Error("test delete error,delete failed")
	}
}

func getMysqlData(id string)  (map[string]interface{},error){
	schema := SchemaName
	table := TableName
	conn := mysql.NewConnect(url)
	Fields := history.GetSchemaTableFieldList(conn,schema,table)
	sql := ""
	for index,Field := range Fields{
		if index == 0 {
			sql = Field.COLUMN_NAME
		}else{
			sql += ","+Field.COLUMN_NAME
		}
	}
	sql = "select "+sql +" from `"+schema+"`.`"+table +"` where id = "+id

	//sql := "select id,test_unsinged_bigint,test_unsinged_int,test_unsinged_mediumint,test_unsinged_tinyint,testtinyint,testsmallint,testmediumint,testint,testbigint,testbit,testbool,testvarchar,testchar,testtime,testdate,testyear,testtimestamp,testdatetime,testfloat,testdouble,testdecimal,testtext,testblob,testmediumblob,testlongblob,testtinyblob,testenum,testset from bifrost_test.binlog_field_test where id = 1"

	stmt,err := conn.Prepare(sql)
	if err != nil{
		return nil,err
	}
	rows,err := stmt.Query([]dbDriver.Value{})
	if err != nil{
		return  nil,err
	}
	n := len(Fields)
	m := make(map[string]interface{}, n)
	for {
		dest := make([]dbDriver.Value, n, n)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		for i, v := range Fields {
			if dest[i] == nil {
				m[v.COLUMN_NAME] = nil
				continue
			}
			switch v.DATA_TYPE {
			case "set":
				s := string(dest[i].([]byte))
				m[v.COLUMN_NAME] = strings.Split(s, ",")
				break
			default:
				m[v.COLUMN_NAME], _ = dataType.TransferDataType(dest[i].([]byte), v.ToDataType)
				break
			}
		}
		break
	}

	return m, nil
}

func checkDataRight(eventDataMap map[string]interface{}) (map[string][]string,error) {

	m,err :=getMysqlData(fmt.Sprint(eventDataMap["id"]))
	if err != nil{
		return nil,err
	}

	result := make(map[string][]string,0)
	result["ok"] = make([]string,0)
	result["error"] = make([]string,0)

	for key,val := range eventDataMap{
		if _,ok := m[key];!ok{
			s := fmt.Sprint("field:",key," not esxit")
			result["error"] = append(result["error"],s)
		}
		if reflect.TypeOf(val) == reflect.TypeOf(m[key]) && fmt.Sprint(val) == fmt.Sprint(m[key]){
			s := fmt.Sprint(key," == ",val," ( ",reflect.TypeOf(val)," ) ")
			result["ok"] = append(result["ok"],s)
		}else{
			s := fmt.Sprint(key," ",val," ( ",reflect.TypeOf(val)," ) "," != ",m[key],reflect.TypeOf(m[key]))
			result["error"] = append(result["error"],s)
		}
	}

	return result,nil
}

