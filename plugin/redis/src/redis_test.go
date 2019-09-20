package src_test

import (
	"testing"
	"log"
	"github.com/brokercap/Bifrost/sdk/pluginTestData"
	MyPlugin "github.com/brokercap/Bifrost/plugin/redis/src"
	"github.com/brokercap/Bifrost/test/pluginTest"
	//"github.com/garyburd/redigo/redis"
	"github.com/go-redis/redis"
	"fmt"
	"strings"
)

var url string = "10.40.2.41:6379"

var redisConn *redis.Client
func getParam() map[string]interface{}{
	p := make(map[string]interface{},0)
	p["KeyConfig"] = "{$SchemaName}-{$TableName}-{$id}"
	p["DataType"] = "json"
	p["Type"] = "set"
	p["DataType"] = "json"
	return p
}

func initRedisConn() error{
	redisConn =  redis.NewClient(&redis.Options{
		Addr:     	url,
		Password: 	"", // no password set
		DB:			0,
	})
	if redisConn == nil{
		return fmt.Errorf("connect error")
	}
	return nil
}

func TestChechUri(t *testing.T){
	var url string = "127.0.0.1:6379"
	myConn := MyPlugin.MyConn{}
	if err := myConn.CheckUri(url);err!= nil{
		log.Println("TestChechUri err:",err)
	}else{
		log.Println("TestChechUri success")
	}
}

func TestSetParam(t *testing.T){
	myConn := MyPlugin.MyConn{}
	conn := myConn.Open(url)
	conn.SetParam(getParam())
}

func TestInsert(t *testing.T){
	myConn := MyPlugin.MyConn{}
	conn := myConn.Open(url)
	conn.SetParam(getParam())
	conn.Insert(pluginTest.GetTestInsertData())
	log.Println("test over")
}

func TestUpate(t *testing.T){
	myConn := MyPlugin.MyConn{}
	conn := myConn.Open(url)
	conn.SetParam(getParam())
	conn.Insert(pluginTest.GetTestUpdateData())
	log.Println("test over")
}


func TestDelete(t *testing.T){
	myConn := MyPlugin.MyConn{}
	conn := myConn.Open(url)
	conn.SetParam(getParam())
	conn.Insert(pluginTest.GetTestDeleteData())
	log.Println("test over")
}


func TestQuery(t *testing.T){
	myConn := MyPlugin.MyConn{}
	conn := myConn.Open(url)
	conn.SetParam(getParam())
	conn.Insert(pluginTest.GetTestQueryData())
	log.Println("test over")
}

func TestCommit(t *testing.T){
	myConn := MyPlugin.MyConn{}
	conn := myConn.Open(url)
	conn.Commit()
	log.Println("test over")
}

func TestCheckData(t *testing.T){

	var err error
	err = initRedisConn()
	if err!=nil{
		t.Fatal(err)
	}
	myConn := MyPlugin.MyConn{}
	conn := myConn.Open(url)
	conn.SetParam(getParam())

	e := pluginTestData.NewEvent()

	var checkResult map[string][]string

	t.Log("")
	t.Log("insert test start")

	insertData := e.GetTestInsertData()
	//log.Println(insertData)
	_, err = conn.Insert(insertData)
	if err != nil{
		t.Fatal(err)
	}
	var key string
	key = insertData.SchemaName+"-"+insertData.TableName+"-"+fmt.Sprint(insertData.Rows[0]["id"])
	var c string
	c,err = redisConn.Get( key).Result()
	if err!=nil{
		t.Fatal(err)
	}

	checkResult,err = e.CheckData(insertData.Rows[0],c)
	if err != nil{
		log.Fatal(err)
		t.Fatal(err)
	}


	for _,v := range checkResult["ok"]{
		t.Log(v)
	}

	for _,v := range checkResult["error"]{
		t.Error(v)
	}


	t.Log("")
	t.Log("update test start")

	updateData := e.GetTestUpdateData()
	_, err = conn.Update(updateData)
	if err != nil{
		t.Fatal(err)
	}

	key = updateData.SchemaName+"-"+updateData.TableName+"-"+fmt.Sprint(updateData.Rows[1]["id"])
	c,err = redisConn.Get( key).Result()
	if err!=nil{
		t.Fatal(err)
	}

	checkResult,err = e.CheckData(updateData.Rows[1],c)
	if err != nil{
		t.Fatal(err)
	}

	for _,v := range checkResult["ok"]{
		t.Log(v)
	}

	for _,v := range checkResult["error"]{
		t.Error(v)
	}


	t.Log("")
	t.Log("delete test start")

	deleteData := e.GetTestDeleteData()

	_, err = conn.Del(deleteData)
	if err != nil{
		t.Fatal(err)
	}

	key = deleteData.SchemaName+"-"+deleteData.TableName+"-"+fmt.Sprint(deleteData.Rows[0]["id"])
	c,err = redisConn.Get( key).Result()
	if strings.Contains(fmt.Sprint(err),"redis: nil") {
		t.Log("key:",key, " delete success")
	}else{
		t.Error("key:",key, " delete error,",err)
	}

	log.Println("test over")
}

//模拟正式环境刷数据
func TestSyncLikeProduct(t *testing.T)  {
	p := pluginTestData.NewPlugin("redis",url)
	err0 := p.SetParam(getParam())
	if err0 != nil{
		t.Fatal(err0)
	}

	var n uint = 10
	err := p.DoTestStart(n)

	if err != nil{
		t.Fatal(err)
	}else{
		t.Log("test success")
	}

}


//模拟正式环境性能测试(只随机生成一条数据。循环提交)
func TestSyncLikeProductForSpeed(t *testing.T)  {
	p := pluginTestData.NewPlugin("redis",url)
	err0 := p.SetParam(getParam())
	p.SetEventType(pluginTestData.INSERT)
	if err0 != nil{
		t.Fatal(err0)
	}

	var n uint = 100
	err := p.DoTestStartForSpeed(n)

	if err != nil{
		t.Fatal(err)
	}else{
		t.Log("test success")
	}

}