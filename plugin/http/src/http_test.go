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
	"io/ioutil"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"encoding/json"
)

var lastEvent pluginDriver.PluginDataType
var lastBody string

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
	body,err := ioutil.ReadAll(req.Body)
	var data pluginDriver.PluginDataType
	err = json.Unmarshal(body,&data)
	if err != nil {
		w.WriteHeader(501)
		log.Println("body err:",string(body))
		return
	}
	lastEvent = data
	lastBody = string(body)
	log.Println("body:",string(body))
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
	p := make(map[string]interface{},2)
	p["ContentType"] = "application/json-raw"
	p["Timeout"] = 10
	return p
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

	checkResult,err = e.CheckData2(eventData.Rows[len(eventData.Rows)-1],lastBody)
	if err != nil{
		t.Log(lastBody)
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


	checkResult,err = e.CheckData2(eventData.Rows[len(eventData.Rows)-1],lastBody)
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
