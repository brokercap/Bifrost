package src_test

import (
	"testing"
	"log"
	"github.com/jc3wish/Bifrost/test/pluginTest"
	MyPlugin "github.com/jc3wish/Bifrost/plugin/clickhouse/src"
)

var url string = "tcp://10.40.2.41:9000?Database=testdebug=true&compress=1"

func TestChechUri(t *testing.T){
	myConn := MyPlugin.MyConn{}
	if err := myConn.CheckUri(url);err!= nil{
		log.Println("TestChechUri err:",err)
	}else{
		log.Println("TestChechUri success")
	}
}

type fieldStruct struct {
	CK 		string
	MySQL 	string
}

func Test(t *testing.T){
	myConn := MyPlugin.MyConn{}
	conn := myConn.Open(url)

	param := make(map[string]interface{},0)
	Field := make([]fieldStruct,0)
	Field = append(Field,fieldStruct{"id","id"})
	Field = append(Field,fieldStruct{"testfloat","testfloat"})
	Field = append(Field,fieldStruct{"testtimestamp","testtimestamp"})
	param["Field"] = Field

	PriKey := make([]fieldStruct,1)
	PriKey[0] = fieldStruct{"id","id"}
	param["PriKey"] = PriKey
	param["CkSchema"] = "test"
	param["CkTable"] = "binlog_field_test"

	p,err := conn.SetParam(param)
	if err != nil{
		log.Fatal(err)
	}

	log.Println("p:",p)

	conn.Insert(pluginTest.GetTestInsertData())
	conn.Update(pluginTest.GetTestInsertData())
	_,err2 := conn.Commit()
	if err2 != nil{
		log.Fatal(err2)
	}


}
