package src_test

import (
	"testing"
	"log"
	"github.com/jc3wish/Bifrost/test/pluginTest"
	MyPlugin "github.com/jc3wish/Bifrost/plugin/redis/src"
)

var url string = "127.0.0.1:6379"

func getParam() map[string]interface{}{
	p := make(map[string]interface{},0)
	p["KeyConfig"] = "{$SchemaName}-{$$TableName}-{$id}"
	p["DataType"] = "json"
	p["Type"] = "set"
	p["DataType"] = "json"
	return p
}

func TestChechUri(t *testing.T){
	myConn := MyPlugin.MyConn{}
	if err := myConn.CheckUri(url);err!= nil{
		log.Println("TestChechUri err:",err)
	}else{
		log.Println("TestChechUri success")
	}
}

func TestSetParam(t *testing.T){
	myConn := MyPlugin.MyConn{}
	conn := myConn.Open(url)
	conn.SetParam(getParam())
}

func TestInsert(t *testing.T){
	myConn := MyPlugin.MyConn{}
	conn := myConn.Open(url)
	conn.SetParam(getParam())
	conn.Insert(pluginTest.GetTestInsertData())
	log.Println("test over")
}

func TestUpate(t *testing.T){
	myConn := MyPlugin.MyConn{}
	conn := myConn.Open(url)
	conn.SetParam(getParam())
	conn.Insert(pluginTest.GetTestUpdateData())
	log.Println("test over")
}


func TestDelete(t *testing.T){
	myConn := MyPlugin.MyConn{}
	conn := myConn.Open(url)
	conn.SetParam(getParam())
	conn.Insert(pluginTest.GetTestDeleteData())
	log.Println("test over")
}


func TestQuery(t *testing.T){
	myConn := MyPlugin.MyConn{}
	conn := myConn.Open(url)
	conn.SetParam(getParam())
	conn.Insert(pluginTest.GetTestQueryData())
	log.Println("test over")
}

func TestCommit(t *testing.T){
	myConn := MyPlugin.MyConn{}
	conn := myConn.Open(url)
	conn.Commit()
	log.Println("test over")
}
