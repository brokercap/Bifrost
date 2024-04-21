//go:build integration
// +build integration

package src_test

import (
	"encoding/json"
	MyPlugin "github.com/brokercap/Bifrost/plugin/MongoDB/src"
	"github.com/brokercap/Bifrost/sdk/pluginTestData"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
	"testing"
)

var url = "bifrost_mongodb_test:27017"

var MongodbConn *mgo.Session

func TestChechUri_Integration(t *testing.T) {
	myConn := MyPlugin.NewConn()
	myConn.SetOption(&url, nil)
	if err := myConn.CheckUri(); err != nil {
		t.Error("TestChechUri err:", err)
	} else {
		t.Log("TestChechUri success")
	}
}

func beforetest() {
	var err error
	MongodbConn, err = mgo.Dial(url)
	if err != nil {
		log.Fatal(err)
	}
	//MongodbConn.SetMode(mgo.Monotonic, true)
}

func getDataFromMongodb(Schema string, Table string, id uint32) []pluginTestData.DataStruct {
	c := MongodbConn.DB(Schema).C(Table)
	query := c.Find(bson.M{"id": id})
	var result []pluginTestData.DataStruct
	query.All(&result)
	return result
}

func getParam() map[string]interface{} {
	MongoDBKeyPluginPamram := make(map[string]interface{})
	MongoDBKeyPluginPamram["SchemaName"] = "{$SchemaName}"
	MongoDBKeyPluginPamram["TableName"] = "{$TableName}"
	MongoDBKeyPluginPamram["PrimaryKey"] = "id"
	return MongoDBKeyPluginPamram
}

func TestAndCheck_Integration(t *testing.T) {
	myConn := MyPlugin.NewConn()
	myConn.SetOption(&url, nil)
	myConn.Open()
	myConn.SetParam(getParam())

	e := pluginTestData.NewEvent()

	eventData := e.GetTestInsertData()

	eventData.Rows[0]["test_unsinged_bigint"] = uint64(2147483647)
	_, _, err := myConn.Insert(eventData, false)

	if err != nil {
		t.Log(eventData)
		t.Fatal(err)
	}

	eventData = e.GetTestUpdateData()

	eventData.Rows[1]["test_unsinged_bigint"] = uint64(2147483647)
	_, _, err = myConn.Update(eventData, false)

	if err != nil {
		t.Log(eventData)
		t.Fatal(err)
	}

	myConn.TimeOutCommit()

	result := getDataFromMongodb(eventData.SchemaName, eventData.TableName, eventData.Rows[1]["id"].(uint32))
	if len(result) != 1 {
		t.Fatal("get result from mongodb not ==1", result)
	}

	resulstByte, err := json.Marshal(result[0])
	if err != nil {
		t.Log(result[0])
		t.Fatal(err)
	}

	checkResult, err := e.CheckData(eventData.Rows[len(eventData.Rows)-1], string(resulstByte))

	if err != nil {
		t.Fatal(err)
	}

	for _, v := range checkResult["ok"] {
		t.Log(v)
	}

	for _, v := range checkResult["error"] {
		t.Error(v)
	}

	eventData = e.GetTestDeleteData()
	myConn.Del(eventData, false)
	myConn.TimeOutCommit()

	result2 := getDataFromMongodb(eventData.SchemaName, eventData.TableName, eventData.Rows[0]["id"].(uint32))
	if len(result2) != 0 {
		t.Fatal("get result from mongodb not == 0;delete failed", result)
	}

	t.Log("test over")

}
