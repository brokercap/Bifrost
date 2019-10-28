package src_test

import "testing"

import (
	"github.com/brokercap/Bifrost/test/pluginTest"
	MyPlugin "github.com/brokercap/Bifrost/plugin/TableCount/src"
	"github.com/brokercap/Bifrost/sdk/pluginTestData"
)

var url = "TableCount"

var dbname string = "TestDbName"

func getParam() map[string]interface{}{
	p := make(map[string]interface{},0)
	p["DbName"] = dbname
	return p
}

func TestChechUri(t *testing.T){
	myConn := MyPlugin.MyConn{}
	if err := myConn.CheckUri(url);err!= nil{
		t.Fatal("TestChechUri err:",err)
	}else{
		t.Log("TestChechUri success")
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


//模拟正式环境刷数据
func TestSyncLikeProduct(t *testing.T)  {
	p := pluginTestData.NewPlugin("TableCount",url)
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