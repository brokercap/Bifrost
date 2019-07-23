package src_test

import (
	"testing"
	"log"
	"github.com/brokercap/Bifrost/sdk/pluginTestData"
	MyPlugin "github.com/brokercap/Bifrost/plugin/redis/src"
	"github.com/brokercap/Bifrost/test/pluginTest"
	"github.com/garyburd/redigo/redis"
	"fmt"
	"encoding/json"
	"reflect"
)

var url string = "10.40.2.41:6379"

var redisConn redis.Conn
func getParam() map[string]interface{}{
	p := make(map[string]interface{},0)
	p["KeyConfig"] = "{$SchemaName}-{$TableName}-{$id}"
	p["DataType"] = "json"
	p["Type"] = "set"
	p["DataType"] = "json"
	return p
}

func initRedisConn() error{
	var err error
	redisConn, err = redis.Dial("tcp", url)
	if err != nil{
		return err
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

	log.Println("")
	log.Println("insert test start")

	insertData := e.GetTestInsertData()
	log.Println(insertData)
	_, err = conn.Insert(insertData)
	if err != nil{
		t.Fatal(err)
	}
	var key string
	key = insertData.SchemaName+"-"+insertData.TableName+"-"+fmt.Sprint(insertData.Rows[0]["id"])
	var c string
	c,err = redis.String(redisConn.Do("GET", key))
	if err != nil{
		t.Fatal(err)
	}

	chekcData(insertData.Rows[0],c)

	log.Println("")
	log.Println("update test start")

	updateData := e.GetTestUpdateData()
	_, err = conn.Update(updateData)
	if err != nil{
		t.Fatal(err)
	}

	key = updateData.SchemaName+"-"+updateData.TableName+"-"+fmt.Sprint(updateData.Rows[1]["id"])
	c,err = redis.String(redisConn.Do("GET", key))
	if err != nil{
		t.Fatal(err)
	}

	chekcData(updateData.Rows[1],c)


	log.Println("")
	log.Println("delete test start")

	deleteData := e.GetTestDeleteData()

	_, err = conn.Del(deleteData)
	if err != nil{
		t.Fatal(err)
	}

	key = deleteData.SchemaName+"-"+deleteData.TableName+"-"+fmt.Sprint(deleteData.Rows[0]["id"])
	c,err = redis.String(redisConn.Do("GET", key))
	if err != nil{
		t.Error("key not delete",key,"==",c)
	}

	log.Println("test over")
}

func chekcData(src map[string]interface{},dest string)  {
	var destMap map[string]interface{}
	json.Unmarshal([]byte(dest),&destMap)

	errorList := make([]string,0)
	for k,v := range src{
		if _,ok := destMap[k];!ok{
			errorList = append(errorList,k)
			continue
		}

		if reflect.TypeOf(v) == reflect.TypeOf(destMap[k]){
			if fmt.Sprint(v) == fmt.Sprint(destMap[k]){
				log.Println(k,"==",v)
			}else{
				errorList = append(errorList,k)
			}
		}else{
			errorList = append(errorList,k)
		}
	}
	if len(errorList) > 0 {
		for _, k := range errorList {
			log.Println(k, "value:", src[k], "(", reflect.TypeOf(src[k]), ")", " != ", destMap[k], "(", reflect.TypeOf(destMap[k]), ")")
		}
	}else{
		log.Println(" type and value is all right ")
	}
}