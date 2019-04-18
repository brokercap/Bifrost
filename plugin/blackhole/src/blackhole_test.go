package src_test

import (
	"testing"
	"log"
	"github.com/jc3wish/Bifrost/test/pluginTest"
	MyPlugin "github.com/jc3wish/Bifrost/plugin/http/src"
)

var url string = "http://127.0.0.1:3332/bifrost_http_api_test"

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
	conn.SetParam(nil)
}

func TestInsert(t *testing.T){
	myConn := MyPlugin.MyConn{}
	conn := myConn.Open(url)
	conn.Insert(pluginTest.GetTestInsertData())
}

func TestUpate(t *testing.T){
	myConn := MyPlugin.MyConn{}
	conn := myConn.Open(url)
	conn.Insert(pluginTest.GetTestUpdateData())
}


func TestDelete(t *testing.T){
	myConn := MyPlugin.MyConn{}
	conn := myConn.Open(url)
	conn.Insert(pluginTest.GetTestDeleteData())
}


func TestQuery(t *testing.T){
	myConn := MyPlugin.MyConn{}
	conn := myConn.Open(url)
	conn.Insert(pluginTest.GetTestQueryData())
}

func TestCommit(t *testing.T){
	myConn := MyPlugin.MyConn{}
	conn := myConn.Open(url)
	conn.Commit()
}
