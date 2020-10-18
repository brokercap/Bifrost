package src_test


import (
	"github.com/hprose/hprose-golang/rpc"
	"log"
	//"github.com/brokercap/Bifrost/sdk/pluginTestData"
	MyPlugin "github.com/brokercap/Bifrost/plugin/hprose/src"
	"time"
	"testing"
	"net/http"
	"github.com/brokercap/Bifrost/sdk/pluginTestData"
	"encoding/json"
)

var lastEventData map[string]interface{}
var lastQuery interface{}

var tcpUrl = "tcp4://0.0.0.0:4321/"
var httpUrl = "http://127.0.0.1:8881"

func Check(context *rpc.HTTPContext) (e error) {
	log.Println("Check success")
	return nil
}

func Insert(SchemaName string,TableName string, data map[string]interface{}) (e error) {
	lastEventData = data
	return nil
}

func Update(SchemaName string,TableName string, data []map[string]interface{}) (e error){
	lastEventData = data[1]
	return nil
}

func Delete(SchemaName string,TableName string,data map[string]interface{}) (e error) {
	lastEventData = data
	return nil
}

func Query(SchemaName string,TableName string,data interface{}) (e error) {
	lastQuery = data
	return nil
}

func startTPCServer()  {
	service := rpc.NewTCPServer(tcpUrl)
	service.Debug = true
	service.AddFunction("Insert", Insert)
	service.AddFunction("Update", Update)
	service.AddFunction("Delete", Delete)
	service.AddFunction("Query", Query)
	service.AddFunction("Check", Check)
	service.Start()
}

func startHttpServer()  {
	service := rpc.NewHTTPService()
	service.Debug = true
	service.AddFunction("Insert", Insert)
	service.AddFunction("Update", Update)
	service.AddFunction("Delete", Delete)
	service.AddFunction("ToList", Query)
	service.AddFunction("Check", Check)
	http.ListenAndServe(":8881", service)
}

func beforeTest()  {
	go startTPCServer()
	go startHttpServer()
	time.Sleep(2 * time.Second)
}

func TestCheckUri(t *testing.T)  {
	beforeTest()
	myConn := MyPlugin.MyConn{}
	if err := myConn.CheckUri(tcpUrl);err!= nil{
		t.Error("TCP TestChechUri err:",err)
	}else{
		t.Log("TCP TestChechUri success")
	}

	if err := myConn.CheckUri(httpUrl);err!= nil{
		t.Error("HTTP TestChechUri err:",err)
	}else{
		t.Log("HTTP TestChechUri success")
	}
}

func getParam()  map[string]interface{}{
	return make(map[string]interface{},0)
}

func TestInsertAndCheckData(t *testing.T)  {
	beforeTest()
	myConn := MyPlugin.MyConn{}
	conn := myConn.Open(tcpUrl)
	conn.SetParam(getParam())

	e := pluginTestData.NewEvent()

	var checkResult map[string][]string

	eventData := e.GetTestInsertData()

	conn.Insert(eventData)
	c,err:=json.Marshal(lastEventData)
	if err!=nil{
		t.Fatal(err)
	}

	checkResult,err = e.CheckData(eventData.Rows[len(eventData.Rows)-1],string(c))
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


func TestUpdateAndCheckData(t *testing.T)  {
	beforeTest()
	myConn := MyPlugin.MyConn{}
	conn := myConn.Open(tcpUrl)
	conn.SetParam(getParam())

	e := pluginTestData.NewEvent()

	var checkResult map[string][]string

	eventData := e.GetTestUpdateData()

	conn.Update(eventData)
	c,err:=json.Marshal(lastEventData)
	if err!=nil{
		t.Fatal(err)
	}

	checkResult,err = e.CheckData(eventData.Rows[len(eventData.Rows)-1],string(c))
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


func TestDelAndCheckData(t *testing.T)  {
	beforeTest()
	myConn := MyPlugin.MyConn{}
	conn := myConn.Open(tcpUrl)
	conn.SetParam(getParam())

	e := pluginTestData.NewEvent()

	var checkResult map[string][]string

	eventData := e.GetTestDeleteData()

	conn.Del(eventData)
	c,err:=json.Marshal(lastEventData)
	if err!=nil{
		t.Fatal(err)
	}

	checkResult,err = e.CheckData(eventData.Rows[len(eventData.Rows)-1],string(c))
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


func TestQuery(t *testing.T)  {
	beforeTest()
	myConn := MyPlugin.MyConn{}
	conn := myConn.Open(tcpUrl)
	conn.SetParam(getParam())

	e := pluginTestData.NewEvent()

	eventData := e.GetTestQueryData()

	conn.Query(eventData)
	if lastQuery == nil{
		t.Fatal("test query error,query is nil")
	}
	t.Log(lastQuery)

}