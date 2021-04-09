package src_test

import (
	MyPlugin "github.com/brokercap/Bifrost/plugin/kafka/src"
	"testing"
)

import (
	"github.com/Shopify/sarama"
	"github.com/brokercap/Bifrost/sdk/pluginTestData"
)

func getParam() map[string]interface{} {
	p := make(map[string]interface{}, 3)
	p["Topic"] = "mytestTopic2"
	p["Key"] = "mytestTopic2"
	p["BatchSize"] = 1
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
	myConn.TimeOutCommit()

	if err != nil {
		t.Fatal(err)
	}

	t.Log("insert test over")
}
