//go:build integration
// +build integration

package src_test

import (
	"testing"

	MyPlugin "github.com/brokercap/Bifrost/plugin/kafka/src"
	"github.com/brokercap/Bifrost/sdk/pluginTestData"
)

func getParam() map[string]interface{} {
	p := make(map[string]interface{}, 3)
	p["Topic"] = "mytestTopic3"
	p["Key"] = "mytestTopic2"
	p["BatchSize"] = 1
	p["BifrostMustBeSuccess"] = true
	return p
}

func getKafkaUrls() *string {
	url := "127.0.0.1:9092"
	return &url
}

func TestInsert(t *testing.T) {
	myConn := MyPlugin.NewConn()
	myConn.SetOption(getKafkaUrls(), nil)
	myConn.SetParam(getParam())

	e := pluginTestData.NewEvent()

	var err error

	t.Log(" insert test start")
	eventData := e.GetTestInsertData()

	_, _, err = myConn.Insert(eventData, false)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = myConn.Commit(e.GetTestCommitData(), false)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = myConn.TimeOutCommit()

	if err != nil {
		t.Fatal(err)
	}

	t.Log("insert test over")
}

func TestConn_CheckUri(t *testing.T) {
	c := &MyPlugin.Conn{
		Uri: getKafkaUrls(),
	}
	err := c.CheckUri()
	if err != nil {
		t.Fatal(err)
	}
}
