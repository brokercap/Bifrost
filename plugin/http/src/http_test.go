package src_test

import (
	"testing"
	"log"
	MyPlugin "github.com/brokercap/Bifrost/plugin/http/src"
)


import (
	"net/http"
	"github.com/brokercap/Bifrost/sdk/pluginTestData"
	"time"
)


var lastEventData string
var lastSchemaName string
var lastTableNamee string

type lastEventDataStruct struct {
	SchemaName string
	TableName string
	EventType string
	Data string
}

var lastEvent lastEventDataStruct

func handel_data(w http.ResponseWriter,req *http.Request){
	switch req.Method {
	case "GET":
		check_uri()
		break
	case "POST":
		post(w,req)
		break
	default:
		log.Println("Methon:",req.Method," not supported ")
		break
	}
	w.Write([]byte("success"))
}

func check_uri()  {
	log.Println("check uri success")
	return
}

func post(w http.ResponseWriter,req *http.Request)  {
	req.ParseForm()

	lastEvent = lastEventDataStruct{
		SchemaName:req.Form.Get("SchemaName"),
		TableName:req.Form.Get("TableName"),
		EventType:req.Form.Get("EventType"),
		Data:req.Form.Get("Data"),
	}

	//log.Println("EventType",req.Form.Get("EventType"))
	//log.Println("SchemaName",req.Form.Get("SchemaName"))
	//log.Println("TableName",req.Form.Get("TableName"))
	return
}


var httpUrl string = "http://127.0.0.1:3332/bifrost_http_api_test"
func beforeTest() {
	http.HandleFunc("/bifrost_http_api_test",handel_data)
	go http.ListenAndServe("0.0.0.0:3332", nil)
	time.Sleep(1 * time.Second)
}


func TestChechUri(t *testing.T){
	beforeTest()
	myConn := MyPlugin.MyConn{}
	if err := myConn.CheckUri(httpUrl);err!= nil{
		log.Println("TestChechUri err:",err)
	}else{
		log.Println("TestChechUri success")
	}
}

func getParam() map[string]interface{} {
	return nil
}

func TestSetParam(t *testing.T){
	myConn := MyPlugin.MyConn{}
	conn := myConn.Open(httpUrl)
	conn.SetParam(nil)
}

func TestCommit(t *testing.T){
	myConn := MyPlugin.MyConn{}
	conn := myConn.Open(httpUrl)
	conn.Commit()
}


func TestAndCheckData(t *testing.T)  {
	beforeTest()
	myConn := MyPlugin.MyConn{}
	conn := myConn.Open(httpUrl)
	conn.SetParam(getParam())

	e := pluginTestData.NewEvent()

	var checkResult map[string][]string


	t.Log(" insert test start")
	eventData := e.GetTestInsertData()

	conn.Insert(eventData)

	var err error

	checkResult,err = e.CheckData(eventData.Rows[len(eventData.Rows)-1],lastEvent.Data)
	if err != nil{
		t.Log(lastEvent.Data)
		t.Fatal(err)
	}

	for _,v := range checkResult["ok"]{
		t.Log(v)
	}

	for _,v := range checkResult["error"]{
		t.Error(v)
	}

	t.Log("insert test over")

	t.Log("")
	t.Log(" update test start")
	eventData = e.GetTestUpdateData()

	conn.Update(eventData)

	if eventData.EventType != lastEvent.EventType{
		t.Error("lastEvent.EventType:",lastEvent.EventType, " != ",eventData.EventType )
	}

	t.Log("update test over")


	t.Log("")
	t.Log(" delete test start")
	eventData = e.GetTestDeleteData()

	conn.Del(eventData)


	checkResult,err = e.CheckData(eventData.Rows[len(eventData.Rows)-1],lastEvent.Data)
	if err != nil{
		t.Log(lastEvent)
		t.Fatal(err)
	}

	for _,v := range checkResult["ok"]{
		t.Log(v)
	}

	for _,v := range checkResult["error"]{
		t.Error(v)
	}

	t.Log("delete test over")


}
