//go:build integration
// +build integration

package src_test

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"testing"
	"time"

	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	MyPlugin "github.com/brokercap/Bifrost/plugin/http/src"
	"github.com/brokercap/Bifrost/sdk/pluginTestData"
)

func TestMain(m *testing.M) {
	beforeTest()
	m.Run()
}

var lastEvent pluginDriver.PluginDataType
var lastBody string

func handel_data(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		check_uri()
		break
	case "POST":
		post(w, req)
		break
	default:
		log.Println("Methon:", req.Method, " not supported ")
		break
	}
	w.Write([]byte("success"))
}

func check_uri() {
	log.Println("check uri success")
	return
}

func post(w http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Println("req err:", err)
	}
	var data pluginDriver.PluginDataType
	err = json.Unmarshal(body, &data)
	if err != nil {
		w.WriteHeader(501)
		log.Println("body err:", string(body))
		return
	}
	lastEvent = data
	lastBody = string(body)
	log.Println("body:", string(body))
	return

}

var httpUrl string = "http://127.0.0.1:3332/bifrost_http_api_test"

func beforeTest() {
	http.HandleFunc("/bifrost_http_api_test", handel_data)
	go http.ListenAndServe("0.0.0.0:3332", nil)
	time.Sleep(1 * time.Second)
}

func TestChechUri(t *testing.T) {
	myConn := MyPlugin.NewConn()
	myConn.SetOption(&httpUrl, nil)
	if err := myConn.CheckUri(); err != nil {
		t.Fatal("TestChechUri err:", err)
	} else {
		t.Log("TestChechUri success")
	}
}

func getParam() map[string]interface{} {
	p := make(map[string]interface{}, 2)
	p["ContentType"] = "application/json-raw"
	p["Timeout"] = 10
	p["BifrostFilterQuery"] = false
	return p
}

func TestSetParam(t *testing.T) {
	myConn := MyPlugin.NewConn()
	myConn.SetOption(&httpUrl, nil)
	myConn.Open()
	p, err := myConn.SetParam(getParam())
	if err != nil {
		t.Fatal(err)
	}
	if p == nil {
		t.Fatal("http SetParam err!")
	}
	myConn.SetParam(p)
	if err != nil {
		t.Fatal(err)
	}
	if p == nil {
		t.Fatal("http SetParam err!")
	}
	t.Log("http SetParam success")

}

func TestCommit(t *testing.T) {
	myConn := MyPlugin.NewConn()
	myConn.SetOption(&httpUrl, nil)
	myConn.Open()
	myConn.SetParam(getParam())
	e := pluginTestData.NewEvent()
	_, _, err := myConn.Commit(e.GetTestCommitData(), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("success")
}

func TestAndCheckData(t *testing.T) {
	myConn := MyPlugin.NewConn()
	myConn.SetOption(&httpUrl, nil)
	myConn.Open()
	myConn.SetParam(getParam())

	e := pluginTestData.NewEvent()

	var checkResult map[string][]string

	t.Log(" insert test start")
	eventData := e.GetTestInsertData()

	_, _, err := myConn.Insert(eventData, false)
	if err != nil {
		t.Fatal(err)
	}

	checkResult, err = e.CheckData2(eventData.Rows[len(eventData.Rows)-1], lastBody)
	if err != nil {
		t.Log(lastBody)
		t.Fatal(err)
	}

	for _, v := range checkResult["ok"] {
		t.Log(v)
	}

	for _, v := range checkResult["error"] {
		t.Error(v)
	}

	t.Log("insert test over")

	t.Log("")
	t.Log(" update test start")
	eventData = e.GetTestUpdateData()

	myConn.Update(eventData, false)

	if eventData.EventType != lastEvent.EventType {
		t.Error("lastEvent.EventType:", lastEvent.EventType, " != ", eventData.EventType)
	}

	t.Log("update test over")

	t.Log("")
	t.Log(" delete test start")
	eventData = e.GetTestDeleteData()

	myConn.Del(eventData, false)

	checkResult, err = e.CheckData2(eventData.Rows[len(eventData.Rows)-1], lastBody)
	if err != nil {
		t.Log(lastEvent)
		t.Fatal(err)
	}

	for _, v := range checkResult["ok"] {
		t.Log(v)
	}

	for _, v := range checkResult["error"] {
		t.Error(v)
	}

	t.Log("delete test over")

}

func Test_GetUriParam(t *testing.T) {
	var caseArr = []struct {
		uri  string
		user string
		pwd  string
		url  string
	}{
		{
			uri:  "demo:demo123@http://192.168.64.168:8080/data-api/api/allen-studio/test/bifrost",
			user: "demo",
			pwd:  "demo123",
			url:  "http://192.168.64.168:8080/data-api/api/allen-studio/test/bifrost",
		},
		{
			uri:  "http://192.168.64.168:8080/data-api/api/allen-studio/test/bifrost",
			user: "",
			pwd:  "",
			url:  "http://192.168.64.168:8080/data-api/api/allen-studio/test/bifrost",
		},
		{
			uri:  "demo:@http://192.168.64.168:8080/data-api/api/allen-studio/test/bifrost",
			user: "demo",
			pwd:  "",
			url:  "http://192.168.64.168:8080/data-api/api/allen-studio/test/bifrost",
		},
	}

	for _, caseInfo := range caseArr {
		user, pwd, newUrl := MyPlugin.GetUriParam(caseInfo.uri)
		if user != caseInfo.user {
			t.Fatalf("userName: %s != %s ", user, caseInfo.user)
		}
		if pwd != caseInfo.pwd {
			t.Fatalf("password: %s != %s ", pwd, caseInfo.pwd)
		}
		if newUrl != caseInfo.url {
			t.Fatalf("url: %s != %s ", newUrl, caseInfo.url)
		}
		t.Log("success")
	}

}
