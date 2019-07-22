package src_test

import (
	"testing"
	"log"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"github.com/brokercap/Bifrost/test/pluginTest"
	MyPlugin "github.com/brokercap/Bifrost/plugin/clickhouse/src"
	"time"
	"fmt"
	"reflect"
	"github.com/brokercap/Bifrost/sdk/pluginTestData"
)

var url string = "tcp://10.40.2.41:9000?Database=test&debug=true&compress=1"


//var createTable = "CREATE TABLE binlog_field_test(id UInt32,testtinyint Int8,testsmallint Int16,testmediumint Int32,testint Int32,testbigint Int64,testvarchar String,testchar String,testenum String,testset String,testtime String,testdate Date,testyear Int16,testtimestamp DateTime,testdatetime DateTime,testfloat Float64,testdouble Float64,testdecimal Float64,testtext String,testblob String,testbit Int64,testbool Int8,testmediumblob String,testlongblob String,testtinyblob String,test_unsinged_tinyint UInt8,test_unsinged_smallint UInt16,test_unsinged_mediumint UInt32,test_unsinged_int UInt32,test_unsinged_bigint UInt64) ENGINE = MergeTree() ORDER BY (id);"

var myConn MyPlugin.MyConn
var conn pluginDriver.ConnFun

 func testBefore(){
	 myConn := MyPlugin.MyConn{}
	 conn = myConn.Open(url)
 }

func TestChechUri(t *testing.T){
	myConn := MyPlugin.MyConn{}
	if err := myConn.CheckUri(url);err!= nil{
		log.Println("TestChechUri err:",err)
	}else{
		log.Println("TestChechUri success")
	}
}

func TestGetSchemaList(t *testing.T)  {
	c := MyPlugin.NewClickHouseDBConn(url)
	log.Println(c.GetSchemaList())
}


func TestGetSchemaTableList(t *testing.T)  {
	c := MyPlugin.NewClickHouseDBConn(url)
	log.Println(c.GetSchemaTableList("test"))
}

func TestGetTableFields(t *testing.T)  {
	c := MyPlugin.NewClickHouseDBConn(url)
	log.Println(c.GetTableFields("test.binlog_field_test"))
}

func getPluginConn() pluginDriver.ConnFun {
	type fieldStruct struct {
		CK 		string
		MySQL 	string
	}

	param := make(map[string]interface{},0)
	Field := make([]fieldStruct,0)
	Field = append(Field,fieldStruct{"id","id"})
	Field = append(Field,fieldStruct{"test_unsinged_bigint","test_unsinged_bigint"})
	Field = append(Field,fieldStruct{"test_unsinged_int","test_unsinged_int"})
	Field = append(Field,fieldStruct{"test_unsinged_mediumint","test_unsinged_mediumint"})
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

	param["Field"] = Field

	PriKey := make([]fieldStruct,1)
	PriKey[0] = fieldStruct{"id","id"}
	param["PriKey"] = PriKey
	param["CkSchema"] = "test"
	param["CkTable"] = "binlog_field_test"

	p,err := conn.SetParam(param)
	if err != nil{
		log.Println("set param fatal err")
		log.Fatal(err)
	}

	log.Println("p:",p)
	return conn
}


func TestCommit(t *testing.T){
	testBefore()
	conn := getPluginConn()
	insertdata := pluginTest.GetTestInsertData()
	log.Println("testtimestamp",insertdata.Rows[0]["testtimestamp"])
	conn.Insert(insertdata)
	//conn.Del(pluginTest.GetTestDeleteData())
	//conn.Update(pluginTest.GetTestUpdateData())
	_,err2 := conn.Commit()
	if err2 != nil{
		log.Fatal(err2)
	}
}

func TestReConnCommit(t *testing.T){
	conn := getPluginConn()
	conn.Insert(pluginTest.GetTestInsertData())
	_,err1:=conn.Commit()
	if err1 != nil{
		log.Println("err1",err1)
		return
	}else{
		log.Println("insert 1 success")
	}

	conn.Del(pluginTest.GetTestDeleteData())
	conn.Update(pluginTest.GetTestUpdateData())
	time.Sleep(20 * time.Second)
	for{
		time.Sleep(3 * time.Second)
		_,err2 := conn.Commit()
		if err2 != nil{
			log.Println("err2:",err2)
		}else{
			break
		}
	}
	log.Println("success")
}

func TestCommitAndCheckData(t *testing.T){
	testBefore()
	getPluginConn()
	event := pluginTestData.NewEvent()
	event.SetSchema("test")
	eventData := event.GetTestUpdateData()
	conn.Update(eventData)
	_,err2 := conn.Commit()
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

	for columnName,v := range dataList[0]{
		if _,ok:=m[columnName];!ok{
			t.Error(columnName," not exsit")
		}
		if fmt.Sprint(v) != fmt.Sprint(dataList[0][columnName]){
			t.Error(columnName," ",v,"(",reflect.TypeOf(v),")"," != ",dataList[0][columnName],"(",reflect.TypeOf(dataList[0][columnName]),")")
		}
	}

}
