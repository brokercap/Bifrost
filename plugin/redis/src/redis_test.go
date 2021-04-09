package src_test

import (
	"context"
	"fmt"
	MyPlugin "github.com/brokercap/Bifrost/plugin/redis/src"
	"github.com/brokercap/Bifrost/sdk/pluginTestData"
	"github.com/go-redis/redis/v8"
	"log"
	"strings"
	"testing"
)

var url string = "192.168.220.130:6379"
var event *pluginTestData.Event
var SchemaName = "bifrost_test"
var TableName = "binlog_field_test"
var ctx = context.Background()

func testBefore() {
	event = pluginTestData.NewEvent()
	event.SetSchema(SchemaName)
	event.SetTable(TableName)
}

var redisConn *redis.Client

func getParam() map[string]interface{} {
	p := make(map[string]interface{}, 0)
	p["KeyConfig"] = "{$SchemaName}-{$TableName}-{$id}"
	p["DataType"] = "json"
	p["Type"] = "set"
	p["DataType"] = "json"
	return p
}

func initRedisConn() error {
	redisConn = redis.NewClient(&redis.Options{
		Addr:     url,
		Password: "", // no password set
		DB:       0,
	})
	if redisConn == nil {
		return fmt.Errorf("connect error")
	}
	return nil
}

func TestChechUri(t *testing.T) {
	testBefore()
	var url string = "192.168.220.130:6379"
	myConn := MyPlugin.NewConn()
	myConn.SetOption(&url, nil)
	if err := myConn.CheckUri(); err != nil {
		log.Println("TestChechUri err:", err)
	} else {
		log.Println("TestChechUri success")
	}
}

func TestSetParam(t *testing.T) {
	myConn := MyPlugin.NewConn()
	myConn.SetOption(&url, nil)
	_, err := myConn.SetParam(getParam())
	if err != nil {
		t.Fatal(err)
	}
	t.Log("success")
}

func TestInsert(t *testing.T) {
	testBefore()
	myConn := MyPlugin.NewConn()
	myConn.SetOption(&url, nil)
	myConn.Open()
	_, err := myConn.SetParam(getParam())
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = myConn.Insert(event.GetTestInsertData(), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("success")
}

func TestUpate(t *testing.T) {
	testBefore()
	myConn := MyPlugin.NewConn()
	myConn.SetOption(&url, nil)
	myConn.Open()
	_, err := myConn.SetParam(getParam())
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = myConn.Update(event.GetTestUpdateData(), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("success")
}

func TestDelete(t *testing.T) {
	testBefore()
	myConn := MyPlugin.NewConn()
	myConn.SetOption(&url, nil)
	myConn.Open()
	_, err := myConn.SetParam(getParam())
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = myConn.Del(event.GetTestDeleteData(), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("success")
}

func TestQuery(t *testing.T) {
	testBefore()
	myConn := MyPlugin.NewConn()
	myConn.SetOption(&url, nil)
	myConn.Open()
	_, err := myConn.SetParam(getParam())
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = myConn.Query(event.GetTestQueryData(), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("success")
}

func TestCommit(t *testing.T) {
	testBefore()
	myConn := MyPlugin.NewConn()
	myConn.SetOption(&url, nil)
	myConn.Open()
	_, err := myConn.SetParam(getParam())
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = myConn.Commit(event.GetTestCommitData(), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("success")
}

func TestCheckData(t *testing.T) {
	testBefore()
	var err error
	err = initRedisConn()
	if err != nil {
		t.Fatal(err)
	}
	myConn := MyPlugin.NewConn()
	myConn.SetOption(&url, nil)
	myConn.Open()
	_, err = myConn.SetParam(getParam())
	if err != nil {
		t.Fatal(err)
	}

	e := pluginTestData.NewEvent()

	var checkResult map[string][]string

	t.Log("")
	t.Log("insert test start")

	insertData := e.GetTestInsertData()
	//log.Println(insertData)
	_, _, err = myConn.Insert(insertData, false)
	if err != nil {
		t.Fatal(err)
	}
	var key string
	key = insertData.SchemaName + "-" + insertData.TableName + "-" + fmt.Sprint(insertData.Rows[0]["id"])
	var c string
	c, err = redisConn.Get(ctx, key).Result()
	if err != nil {
		t.Fatal(err)
	}

	checkResult, err = e.CheckData(insertData.Rows[0], c)
	if err != nil {
		log.Fatal(err)
		t.Fatal(err)
	}

	for _, v := range checkResult["ok"] {
		t.Log(v)
	}

	for _, v := range checkResult["error"] {
		t.Error(v)
	}

	t.Log("")
	t.Log("update test start")

	updateData := e.GetTestUpdateData()
	_, _, err = myConn.Update(updateData, false)
	if err != nil {
		t.Fatal(err)
	}

	key = updateData.SchemaName + "-" + updateData.TableName + "-" + fmt.Sprint(updateData.Rows[1]["id"])
	c, err = redisConn.Get(ctx, key).Result()
	if err != nil {
		t.Fatal(err)
	}

	checkResult, err = e.CheckData(updateData.Rows[1], c)
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range checkResult["ok"] {
		t.Log(v)
	}

	for _, v := range checkResult["error"] {
		t.Error(v)
	}

	t.Log("")
	t.Log("delete test start")

	deleteData := e.GetTestDeleteData()

	_, _, err = myConn.Del(deleteData, false)
	if err != nil {
		t.Fatal(err)
	}

	key = deleteData.SchemaName + "-" + deleteData.TableName + "-" + fmt.Sprint(deleteData.Rows[0]["id"])
	c, err = redisConn.Get(ctx, key).Result()
	if strings.Contains(fmt.Sprint(err), "redis: nil") {
		t.Log("key:", key, " delete success")
	} else {
		t.Error("key:", key, " delete error,", err)
	}

	log.Println("test over")
}

//模拟正式环境刷数据
func TestSyncLikeProduct(t *testing.T) {
	p := pluginTestData.NewPlugin("redis", url)
	err0 := p.SetParam(getParam())
	if err0 != nil {
		t.Fatal(err0)
	}

	var n uint = 10
	err := p.DoTestStart(n)

	if err != nil {
		t.Fatal(err)
	} else {
		t.Log("test success")
	}

}

//模拟正式环境性能测试(只随机生成一条数据。循环提交)
func TestSyncLikeProductForSpeed(t *testing.T) {
	p := pluginTestData.NewPlugin("redis", url)
	err0 := p.SetParam(getParam())
	p.SetEventType(pluginTestData.INSERT)
	if err0 != nil {
		t.Fatal(err0)
	}

	var n uint = 100
	err := p.DoTestStartForSpeed(n)

	if err != nil {
		t.Fatal(err)
	} else {
		t.Log("test success")
	}

}

func TestGetUriParam(t *testing.T) {
	url := "pwd@123@tcp(127.0.0.1:6379,127.0.0.1:6380)/16"
	pwd, network, uri, database := MyPlugin.GetUriParam(url)

	t.Log("pwd:", pwd)
	t.Log("network:", network)
	t.Log("uri:", uri)
	t.Log("database:", database)

	url = "127.0.0.1:6379"
	pwd, network, uri, database = MyPlugin.GetUriParam(url)

	t.Log("pwd:", pwd)
	t.Log("network:", network)
	t.Log("uri:", uri)
	t.Log("database:", database)
}
