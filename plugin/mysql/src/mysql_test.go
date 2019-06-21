package src_test

import (
	"testing"
	"log"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"github.com/brokercap/Bifrost/test/pluginTest"
	MyPlugin "github.com/brokercap/Bifrost/plugin/mysql/src"
	"time"
	dbDriver "database/sql/driver"
	"github.com/brokercap/Bifrost/Bristol/mysql"
	"strings"
	"github.com/brokercap/Bifrost/util/dataType"
	"github.com/brokercap/Bifrost/server/history"
	"reflect"
	"fmt"
)

var url string = "root:root123@tcp(10.40.6.89:3306)/bifrost_test"

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
	log.Println(c.GetTableFields("bifrost_test","binlog_field_test"))
}

func getPluginConn() pluginDriver.ConnFun {
	type fieldStruct struct {
		ToField 		string
		FromMysqlField 	string
	}

	myConn := MyPlugin.MyConn{}
	conn := myConn.Open(url)

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

	sql := ""
	for _,f := range Field{
		sql += f.ToField+","
	}
	log.Println(sql)
	param["Field"] = Field

	PriKey := make([]fieldStruct,1)
	PriKey[0] = fieldStruct{"id","id"}
	param["PriKey"] = PriKey
	param["Schema"] = "bifrost_test"
	param["Table"] = "binlog_field_test"

	p,err := conn.SetParam(param)
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


func TestCheckDataRight(t *testing.T)  {

	insertdata := pluginTest.GetTestUpdateData()
	fmap := insertdata.Rows[1]

	schema := "bifrost_test"
	table := "binlog_field_test"
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
	sql = "select "+sql +" from "+schema+"."+table +" where id = 1"

	//sql := "select id,test_unsinged_bigint,test_unsinged_int,test_unsinged_mediumint,test_unsinged_tinyint,testtinyint,testsmallint,testmediumint,testint,testbigint,testbit,testbool,testvarchar,testchar,testtime,testdate,testyear,testtimestamp,testdatetime,testfloat,testdouble,testdecimal,testtext,testblob,testmediumblob,testlongblob,testtinyblob,testenum,testset from bifrost_test.binlog_field_test where id = 1"

	stmt,err := conn.Prepare(sql)
	if err != nil{
		log.Fatal(err)
	}
	rows,err := stmt.Query([]dbDriver.Value{})
	if err != nil{
		log.Fatal(err)
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

	var typeof = func(v interface{}) string {
		return reflect.TypeOf(v).String()
	}


	valTest := true
	for key,val := range fmap{
		if _,ok := m[key];!ok{
			log.Fatal("field:",key," not esxit")
		}

		if typeof(val) != typeof(m[key]){
			log.Println("field:",key," 类型不对", "写入类型:",typeof(val)," 读出类型:",typeof(m[key]))
		}

		if key=="testfloat"{
			if fmt.Sprint(val) != fmt.Sprint(m[key]) {
				valTest =false
				log.Println("field:",key," 值不对", "写入:",val," 读出:",m[key])
			}
		}else if key == "testset"{
			if fmt.Sprint(val) != fmt.Sprint(m[key]){
				valTest =false
				log.Println("field:",key," 值不对", "写入:",val," 读出:",m[key])
			}
		} else{
			if val != m[key]{
				valTest =false
				log.Println("field:",key," 值不对", "写入:",val," 读出:",m[key])
			}
		}
	}

	if valTest {
		log.Println(" success over, val is all right")
	}else{
		log.Println(" success over")
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

func TestMysqlInsert(t *testing.T) {
	sql := "REPLACE INTO bifrost_test.binlog_field_test (id,testtinyint,testsmallint,testmediumint,testint,testbigint,testvarchar,testchar,testenum,testset,testtime,testdate,testyear,testtimestamp,testdatetime,testfloat,testdouble,testdecimal,testtext,testblob,testbit,testbool,testmediumblob,testlongblob,testtinyblob,test_unsinged_tinyint,test_unsinged_smallint,test_unsinged_mediumint,test_unsinged_int,test_unsinged_bigint) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	conn := mysql.NewConnect(url)
	stmt,err := conn.Prepare(sql)
	if err!=nil{
		log.Fatal(err)
	}

	val := make([]dbDriver.Value,30)
	//id,testtinyint,testsmallint,testmediumint,testint,testbigint,testvarchar,testchar,testenum,testset,testtime,testdate,testyear,testtimestamp,testdatetime,testfloat,testdouble,testdecimal,testtext,testblob,testbit,testbool,testmediumblob,testlongblob,testtinyblob,test_unsinged_tinyint,test_unsinged_smallint,test_unsinged_mediumint,test_unsinged_int,test_unsinged_bigint
	//2 -1 -2 -3 -4 -5 2 2 en1 set1 NULL NULL 1989 2019-06-20 14:55:07 NULL 0 0 0.00 2 2 0 0 2 2 2 1 2 3 4 5
	val[0] = "2"
	val[1] = "-1"
	val[2] = "-2"
	val[3] = "-3"
	val[4] = "-4"
	val[5] = "-5"
	val[6] = "2"
	val[7] = "2"
	val[8] = "en1"
	val[9]= "set1"
	val[10]= nil
	val[11]= nil
	val[12]= "1989"

	val[13]= "2019-06-20 14:55:07"
	val[14]= "NULL"
	val[15]= "0"
	val[16]= "0"
	val[17]= "0.00"
	val[18]= "2"
	val[19]= "2"
	val[20]= int64(100)
	val[21]= "0"
	val[22]= "2"
	val[23]= "2"
	val[24]= "2"
	val[25]= "1"
	val[26]= "2"
	val[27]= "3"
	val[28]= "4"
	val[29]= "5"

	log.Println("start exec")
	r,err2 := stmt.Exec(val)
	if err2!=nil{
		log.Fatal(err2)
	}
	log.Println("over exec")

	log.Println(r)

}