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
)

var url string = "tcp://10.40.2.41:9000?Database=test&debug=true&compress=1"


//var createTable = "CREATE TABLE binlog_field_test(id UInt32,testtinyint Int8,testsmallint Int16,testmediumint Int32,testint Int32,testbigint Int64,testvarchar String,testchar String,testenum String,testset String,testtime String,testdate Date,testyear Int16,testtimestamp DateTime,testdatetime DateTime,testfloat Float64,testdouble Float64,testdecimal Float64,testtext String,testblob String,testbit Int64,testbool Int8,testmediumblob String,testlongblob String,testtinyblob String,test_unsinged_tinyint UInt8,test_unsinged_smallint UInt16,test_unsinged_mediumint UInt32,test_unsinged_int UInt32,test_unsinged_bigint UInt64) ENGINE = MergeTree() ORDER BY (id);"
/*

CREATE TABLE binlog_field_test(id UInt32,testtinyint Int8,testsmallint Int16,testmediumint Int32,testint Int32,testbigint Int64,testvarchar String,testchar String,testenum String,testset String,testtime String,testdate Date,testyear Int16,testtimestamp DateTime,testdatetime DateTime,testfloat Float64,testdouble Float64,testdecimal Float64,testtext String,testblob String,testbit Int64,testbool Int8,testmediumblob String,testlongblob String,testtinyblob String,test_unsinged_tinyint UInt8,test_unsinged_smallint UInt16,test_unsinged_mediumint UInt32,test_unsinged_int UInt32,test_unsinged_bigint UInt64) ENGINE = MergeTree() ORDER BY (id);
*/

var myConn MyPlugin.MyConn
var conn pluginDriver.ConnFun
var event *pluginTestData.Event
var SchemaName = "bifrost_test"
var TableName = "binlog_field_test"

func testBefore(){
	myConn := MyPlugin.MyConn{}
	conn = myConn.Open(url)

	event = pluginTestData.NewEvent()
	event.SetSchema(SchemaName)
	event.SetTable(TableName)
}

func initDBTable(delTable bool) {
	c := MyPlugin.NewClickHouseDBConn(url)
	sql1:= "CREATE DATABASE IF NOT EXISTS  `"+SchemaName+"`";
	c.Exec(sql1,[]driver.Value{})
	sql2:="CREATE TABLE IF NOT EXISTS "+SchemaName+"."+TableName+"(id UInt32,testtinyint Int8,testsmallint Int16,testmediumint Int32,testint Int32,testbigint Int64,testvarchar String,testchar String,testenum String,testset String,testtime String,testdate Date,testyear Int16,testtimestamp DateTime,testdatetime DateTime,testfloat Float64,testdouble Float64,testdecimal Float64,testtext String,testblob String,testbit Int64,testbool Int8,testmediumblob String,testlongblob String,testtinyblob String,test_unsinged_tinyint UInt8,test_unsinged_smallint UInt16,test_unsinged_mediumint UInt32,test_unsinged_int UInt32,test_unsinged_bigint UInt64) ENGINE = MergeTree() ORDER BY (id);"
	if delTable == false{
		c.Exec(sql2,[]driver.Value{})
	}else{
		sql3 := "DROP TABLE "+SchemaName+"."+TableName;
		c.Exec(sql3,[]driver.Value{})
		c.Exec(sql2,[]driver.Value{})
	}
	c.Close()
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

func getParam() map[string]interface{} {
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

	param["Field"] = Field

	PriKey := make([]fieldStruct,1)
	PriKey[0] = fieldStruct{"id","id"}
	param["PriKey"] = PriKey
	param["CkSchema"] = SchemaName
	param["CkTable"] = TableName
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


func TestCommit(t *testing.T){
	testBefore()
	initDBTable(true)
	initSyncParam()
	insertdata := event.GetTestInsertData()
	conn.Insert(insertdata)
	conn.Del(event.GetTestDeleteData())
	conn.Update(event.GetTestUpdateData())

	conn.Insert(event.GetTestInsertData())
	conn.Del(event.GetTestDeleteData())
	conn.Insert(event.GetTestInsertData())
	_,err2 := conn.Commit()
	if err2 != nil{
		t.Fatal(err2)
	}

	conn.Del(event.GetTestDeleteData())
	conn.Update(event.GetTestUpdateData())

	conn.Insert(event.GetTestInsertData())
	conn.Del(event.GetTestDeleteData())
	conn.Insert(event.GetTestInsertData())
	conn.Insert(event.GetTestInsertData())
	conn.Insert(event.GetTestInsertData())
	_,err2 = conn.Commit()
	if err2 != nil{
		t.Fatal(err2)
	}
}

func TestReConnCommit(t *testing.T){
	testBefore()
	initDBTable(false)
	initSyncParam()
	conn.Insert(event.GetTestInsertData())
	_,err1:=conn.Commit()
	if err1 != nil{
		log.Println("err1",err1)
		return
	}else{
		log.Println("insert 1 success")
	}

	conn.Del(event.GetTestDeleteData())
	conn.Update(event.GetTestUpdateData())
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
	initDBTable(true)
	initSyncParam()
	event := pluginTestData.NewEvent()
	eventData := event.GetTestInsertData()
	eventData = event.GetTestUpdateData()
	conn.Update(eventData)
	//conn.Insert(eventData)
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

	errDataList := make([]string,0)
	for columnName,v := range dataList[0]{
		if _,ok:=m[columnName];!ok{
			t.Error(columnName," not exsit")
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

			switch v.(type) {
			case float64:
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
		default:

			switch v.(type) {
				case time.Time:
					// 这里用包括关系 ，也是因为 ck 读出来的时候，date和datetime类型都转成了time.Time 类型了
					descTime := fmt.Sprint(v.(time.Time).Format("2006-01-02 15:04:05"))
					if descTime == fmt.Sprint(m[columnName]) || strings.Index(descTime,fmt.Sprint(m[columnName])) == 0{
						result = true
					}else{
						t.Log(columnName,":",descTime)
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
			t.Log(columnName," ",v,"(",reflect.TypeOf(v),")"," == ",m[columnName],"(",reflect.TypeOf(m[columnName]),")")
		}else{
			errDataList = append(errDataList,columnName)
		}
	}

	for _,columnName := range errDataList{
		v := dataList[0][columnName]
		t.Error(columnName," ",v,"(",reflect.TypeOf(v),")"," != ",m[columnName],"(",reflect.TypeOf(m[columnName]),")")
	}

	if len(errDataList) == 0{
		t.Log("test over;", "data is all right")
	}else{
		t.Error("test over;"," some data is error")
	}

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