//go:build integration
// +build integration

package src_test

import (
	"encoding/json"
	"github.com/hprose/hprose-golang/rpc"
	"log"
	//"github.com/brokercap/Bifrost/sdk/pluginTestData"
	MyPlugin "github.com/brokercap/Bifrost/plugin/hprose/src"
	"github.com/brokercap/Bifrost/sdk/pluginTestData"
	"net/http"
	"testing"
	"time"
)

var lastEventData map[string]interface{}
var lastQuery interface{}

var tcpUrl = "tcp4://0.0.0.0:4321/"
var httpUrl = "http://127.0.0.1:8881"

func Check(context *rpc.HTTPContext) (e error) {
	log.Println("Check success")
	return nil
}

func Insert(SchemaName string, TableName string, data map[string]interface{}) (e error) {
	lastEventData = data
	return nil
}

func Update(SchemaName string, TableName string, data []map[string]interface{}) (e error) {
	lastEventData = data[1]
	return nil
}

func Delete(SchemaName string, TableName string, data map[string]interface{}) (e error) {
	lastEventData = data
	return nil
}

func Query(SchemaName string, TableName string, data interface{}) (e error) {
	lastQuery = data
	return nil
}

func startTPCServer() {
	service := rpc.NewTCPServer(tcpUrl)
	service.Debug = true
	service.AddFunction("Insert", Insert)
	service.AddFunction("Update", Update)
	service.AddFunction("Delete", Delete)
	service.AddFunction("Query", Query)
	service.AddFunction("Check", Check)
	service.Start()
}

func startHttpServer() {
	service := rpc.NewHTTPService()
	service.Debug = true
	service.AddFunction("Insert", Insert)
	service.AddFunction("Update", Update)
	service.AddFunction("Delete", Delete)
	service.AddFunction("ToList", Query)
	service.AddFunction("Check", Check)
	http.ListenAndServe(":8881", service)
}

func beforeTest() {
	go startTPCServer()
	go startHttpServer()
	time.Sleep(2 * time.Second)
}

func TestCheckUri(t *testing.T) {
	beforeTest()
	myConn := MyPlugin.NewConn()
	myConn.SetOption(&tcpUrl, nil)
	if err := myConn.CheckUri(); err != nil {
		t.Error("TCP TestChechUri err:", err)
	} else {
		t.Log("TCP TestChechUri success")
	}
	myConn.SetOption(&httpUrl, nil)
	if err := myConn.CheckUri(); err != nil {
		t.Error("HTTP TestChechUri err:", err)
	} else {
		t.Log("HTTP TestChechUri success")
	}
}

func getParam() map[string]interface{} {
	return make(map[string]interface{}, 0)
}

func TestInsertAndCheckData(t *testing.T) {
	beforeTest()
	myConn := MyPlugin.NewConn()
	myConn.SetOption(&tcpUrl, nil)
	myConn.SetParam(getParam())

	e := pluginTestData.NewEvent()

	var checkResult map[string][]string

	eventData := e.GetTestInsertData()

	myConn.Insert(eventData, false)
	c, err := json.Marshal(lastEventData)
	if err != nil {
		t.Fatal(err)
	}

	checkResult, err = e.CheckData(eventData.Rows[len(eventData.Rows)-1], string(c))
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range checkResult["ok"] {
		t.Log(v)
	}

	for _, v := range checkResult["error"] {
		t.Error(v)
	}

}

func TestUpdateAndCheckData(t *testing.T) {
	beforeTest()
	myConn := MyPlugin.NewConn()
	myConn.SetOption(&tcpUrl, nil)
	myConn.SetParam(getParam())

	e := pluginTestData.NewEvent()

	var checkResult map[string][]string

	eventData := e.GetTestUpdateData()

	myConn.Update(eventData, false)
	c, err := json.Marshal(lastEventData)
	if err != nil {
		t.Fatal(err)
	}

	checkResult, err = e.CheckData(eventData.Rows[len(eventData.Rows)-1], string(c))
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range checkResult["ok"] {
		t.Log(v)
	}

	for _, v := range checkResult["error"] {
		t.Error(v)
	}

}

func TestDelAndCheckData(t *testing.T) {
	beforeTest()
	myConn := MyPlugin.NewConn()
	myConn.SetOption(&tcpUrl, nil)
	myConn.SetParam(getParam())

	e := pluginTestData.NewEvent()

	var checkResult map[string][]string

	eventData := e.GetTestDeleteData()

	myConn.Del(eventData, false)
	c, err := json.Marshal(lastEventData)
	if err != nil {
		t.Fatal(err)
	}

	checkResult, err = e.CheckData(eventData.Rows[len(eventData.Rows)-1], string(c))
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range checkResult["ok"] {
		t.Log(v)
	}

	for _, v := range checkResult["error"] {
		t.Error(v)
	}

}

func TestQuery(t *testing.T) {
	beforeTest()
	myConn := MyPlugin.NewConn()
	myConn.SetOption(&tcpUrl, nil)
	myConn.SetParam(getParam())

	e := pluginTestData.NewEvent()

	eventData := e.GetTestQueryData()

	myConn.Query(eventData, false)
	if lastQuery == nil {
		t.Fatal("test query error,query is nil")
	}
	t.Log(lastQuery)
}
