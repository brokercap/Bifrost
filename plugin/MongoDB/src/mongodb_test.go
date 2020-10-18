package src_test

import (
	MyPlugin "github.com/brokercap/Bifrost/plugin/MongoDB/src"
	"gopkg.in/mgo.v2"
	"testing"
	"github.com/brokercap/Bifrost/sdk/pluginTestData"
	"gopkg.in/mgo.v2/bson"
	"encoding/json"
	"log"
)

var url = "10.40.2.41:27017"

var MongodbConn *mgo.Session

func TestChechUri(t *testing.T){
	myConn := MyPlugin.MyConn{}
	if err := myConn.CheckUri(url);err!= nil{
		t.Error("TestChechUri err:",err)
	}else{
		t.Log("TestChechUri success")
	}
}

func beforetest()  {
	var err error
	MongodbConn,err = mgo.Dial(url)
	if err!=nil{
		log.Fatal(err)
	}
	//MongodbConn.SetMode(mgo.Monotonic, true)
}

func getDataFromMongodb(Schema string,Table string,id uint32) []pluginTestData.DataStruct {
	c := MongodbConn.DB(Schema).C(Table)
	query:=c.Find(bson.M{"id":id})
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

func TestAndCheck(t *testing.T)  {
	beforetest()
	myConn := MyPlugin.MyConn{}
	conn := myConn.Open(url)
	conn.SetParam(getParam())

	e := pluginTestData.NewEvent()

	eventData := e.GetTestInsertData()

	eventData.Rows[0]["test_unsinged_bigint"] = uint64(2147483647)
	_,err :=conn.Insert(eventData)

	if err != nil{
		t.Log(eventData)
		t.Fatal(err)
	}

	eventData = e.GetTestUpdateData()

	eventData.Rows[1]["test_unsinged_bigint"] = uint64(2147483647)
	_,err = conn.Update(eventData)

	if err != nil{
		t.Log(eventData)
		t.Fatal(err)
	}

	conn.Commit()

	result := getDataFromMongodb(eventData.SchemaName,eventData.TableName,eventData.Rows[1]["id"].(uint32))
	if len(result) != 1{
		t.Fatal("get result from mongodb not ==1",result)
	}

	resulstByte,err := json.Marshal(result[0])
	if err != nil{
		t.Log(result[0])
		t.Fatal(err)
	}

	checkResult,err:=e.CheckData(eventData.Rows[len(eventData.Rows)-1],string(resulstByte))

	if err != nil{
		t.Fatal(err)
	}

	for _,v := range checkResult["ok"]{
		t.Log(v)
	}

	for _,v := range checkResult["error"]{
		t.Error(v)
	}


	eventData = e.GetTestDeleteData()
	conn.Del(eventData)
	conn.Commit()

	result2 := getDataFromMongodb(eventData.SchemaName,eventData.TableName,eventData.Rows[0]["id"].(uint32))
	if len(result2) != 0{
		t.Fatal("get result from mongodb not == 0;delete failed",result)
	}

	t.Log("test over")

}

