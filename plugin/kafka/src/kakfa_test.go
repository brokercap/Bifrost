package src_test


import (
	"testing"
	MyPlugin "github.com/brokercap/Bifrost/plugin/kafka/src"
)

import (
	"github.com/brokercap/Bifrost/sdk/pluginTestData"
	"github.com/Shopify/sarama"
)

func getParam() map[string]interface{} {
	p := make(map[string]interface{},3)
	p["Topic"] = "mytestTopic2"
	p["Key"] = "mytestTopic2"
	p["BatchSize"] = 1
	return p
}

func getKafkaUrls() string  {
	return 	"127.0.0.1:9092"
}

func TestInsert(t *testing.T)  {
	myConn := MyPlugin.MyConn{}
	conn := myConn.Open(getKafkaUrls())
	conn.SetParam(getParam())

	e := pluginTestData.NewEvent()

	var err error

	t.Log(" insert test start")
	eventData := e.GetTestInsertData()

	_, err = conn.Insert(eventData)
	if err != nil {
		t.Fatal(err)
	}
	_,err = conn.Commit()

	if err != nil {
		t.Fatal(err)
	}

	t.Log("insert test over")
}
