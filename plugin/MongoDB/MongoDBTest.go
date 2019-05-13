package main

import "time"
import (
	"flag"
	"log"
	"github.com/brokercap/Bifrost/test/pluginTest"
	"net/http"
	"strconv"
	"os"
	"syscall"
	"os/signal"
	"encoding/json"
	"runtime"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

func init() {}


var schema_name *string
var table_name *string
var fieldList []string = make([]string,0)
var http_api string = "/bifrost_http_api_test"

type resultStruct struct {
	insert bool
	update bool
	delete bool
	query bool
}

var result *resultStruct
var resultKey *resultStruct
var resultList *resultStruct

var b bool
var httpToServerId int
var MongoDBKeyToServerId int
var MongoDBListToServerId int
var pluginObj *pluginTest.BifrostManager
var MongoDBServer *string
var MongoDBClient  *mgo.Session

func main(){
	result = &resultStruct{
		false,false,false,false,
	}
	resultKey = &resultStruct{
		false,false,false,false,
	}

	bifrost_url := flag.String("bifrost_url", "http://127.0.0.1:21036", "-bifrost_url")
	bifrost_user := flag.String("bifrost_user", "Bifrost", "-bifrost_user")
	bifrost_pwd := flag.String("bifrost_pwd", "Bifrost123", "-bifrost_pwd")
	table_name = flag.String("table", "binlog_field_MongoDB_plugin_test", "-table")
	schema_name = flag.String("schema", "bifrost_test", "-schema")
	pluginServer := flag.String("pluginHttpServer", "127.0.0.1:4324", "-pluginHttpServer")
	MongoDBServer = flag.String("MongoDBServer", "127.0.0.1:27017", "-MongoDBServer")
	DDL := flag.String("ddl", "true", "-ddl")

	mysqluser := flag.String("mysqluser", "root", "-mysqluser")
	mysqlpwd := flag.String("mysqlpwd", "", "-mysqlpwd")
	mysqlhost := flag.String("mysqlhost", "127.0.0.1", "-mysqlhost")
	mysqlport := flag.String("mysqlport", "3306", "-mysqlport")
	mysqldb := flag.String("mysqldb", "test", "-mysqldb")
	flag.Parse()

	dbSourceString := *mysqluser+":"+*mysqlpwd+"@tcp("+*mysqlhost+":"+*mysqlport+")/"+*mysqldb

	var dbName = "MongoDBTest_"+strconv.FormatInt(time.Now().Unix(),10)

	var(
		httpServerKey string = "httpToserverTest_111111"
		MongoDBServerKey string = "MongoDBToserverTest_111111"
		httpPluginPamram map[string]interface{} = make(map[string]interface{},0)
		MongoDBKeyPluginPamram map[string]interface{} = make(map[string]interface{},0)
	)

	MongoDBKeyPluginPamram["SchemaName"] = "{$SchemaName}"
	MongoDBKeyPluginPamram["TableName"] = "{$TableName}"
	MongoDBKeyPluginPamram["PrimaryKey"] = "id"


	fieldList = append(fieldList,"id")
	fieldList = append(fieldList,"testtinyint")
	fieldList = append(fieldList,"testmediumint")
	fieldList = append(fieldList,"testdecimal")
	fieldList = append(fieldList,"testvarchar")
	fieldList = append(fieldList,"testbit")

	var sqlList = []string{
		"CREATE DATABASE /*!32312 IF NOT EXISTS*/ `"+*schema_name+"`",
		"DROP TABLE IF EXISTS "+*schema_name+".`"+*table_name+"`",
		"CREATE TABLE "+*schema_name+".`"+*table_name+"` ("+
			"`id` int(11) unsigned NOT NULL AUTO_INCREMENT,"+
			"`testtinyint` tinyint(4) NOT NULL DEFAULT '-1',"+
			"`testsmallint` smallint(6) NOT NULL DEFAULT '-2',"+
			"`testmediumint` mediumint(8) NOT NULL DEFAULT '-3',"+
			"`testint` int(11) NOT NULL DEFAULT '-4',"+
			"`testbigint` bigint(20) NOT NULL DEFAULT '-5',"+
			"`testvarchar` varchar(10) NOT NULL,"+
			"`testchar` char(2) NOT NULL,"+
			"`testenum` enum('en1','en2','en3') NOT NULL DEFAULT 'en1',"+
			"`testset` set('set1','set2','set3') NOT NULL DEFAULT 'set1',"+
			"`testtime` time NOT NULL DEFAULT '00:00:00',"+
			"`testdate` date NOT NULL DEFAULT '0000-00-00',"+
			"`testyear` year(4) NOT NULL DEFAULT '1989',"+
			"`testtimestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"+
			"`testdatetime` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',"+
			"`testfloat` float(9,2) NOT NULL DEFAULT '0.00',"+
			"`testdouble` double(9,2) NOT NULL DEFAULT '0.00',"+
			"`testdecimal` decimal(9,2) NOT NULL DEFAULT '0.00',"+
			"`testtext` text NOT NULL,"+
			"`testblob` blob NOT NULL,"+
			"`testbit` bit(8) NOT NULL DEFAULT b'0',"+
			"`testbool` tinyint(1) NOT NULL DEFAULT '0',"+
			"`testmediumblob` mediumblob NOT NULL,"+
			"`testlongblob` longblob NOT NULL,"+
			"`testtinyblob` tinyblob NOT NULL,"+
			"`test_unsinged_tinyint` tinyint(4) unsigned NOT NULL DEFAULT '1',"+
			"`test_unsinged_smallint` smallint(6) unsigned NOT NULL DEFAULT '2',"+
			"`test_unsinged_mediumint` mediumint(8) unsigned NOT NULL DEFAULT '3',"+
			"`test_unsinged_int` int(11) unsigned NOT NULL DEFAULT '4',"+
			"`test_unsinged_bigint` bigint(20) unsigned NOT NULL DEFAULT '5',"+
			"PRIMARY KEY (`id`)"+
			") ENGINE=MyISAM AUTO_INCREMENT=2 DEFAULT CHARSET=utf8",
	}

	pluginObj = &pluginTest.BifrostManager{
		Host: *bifrost_url,
		User: *bifrost_user,
		Pwd:  *bifrost_pwd,
		MysqlConn: &pluginTest.MySQLConn{
			Uri: dbSourceString,
		},
	}

	httpToServerUrl := "http://"+*pluginServer+http_api

	pluginObj.Init()

	pluginObj.AddToServer(MongoDBServerKey,"MongoDB",*MongoDBServer,MongoDBServerKey)
	pluginObj.AddToServer(httpServerKey,"http",httpToServerUrl,httpServerKey)

	if *DDL == "true" {
		for _, sql := range sqlList {
			pluginObj.MysqlConn.ExecSQL(sql)
		}
	}

	go httpServer(*pluginServer)

	pluginObj.AddDB(dbName,*bifrost_url)
	pluginObj.AddTable(dbName,*schema_name,*table_name,1)

	b,MongoDBKeyToServerId= pluginObj.AddTableToServer(dbName,*schema_name,*table_name,MongoDBServerKey,"MongoDB",fieldList,1,MongoDBKeyPluginPamram)
	if b == false{
		log.Println(dbName,*schema_name,*table_name,"add MongoDB toserver:",httpServerKey,false)
		runtime.Goexit()
	}

	//给table新增to server
	b,httpToServerId = pluginObj.AddTableToServer(dbName,*schema_name,*table_name,httpServerKey,"http",fieldList,1,httpPluginPamram)
	if b == false{
		log.Println(dbName,*schema_name,*table_name,"add http toserver:",httpServerKey,false)
		runtime.Goexit()
	}

	initMongoDBConn()

	insertSQL()

	pluginObj.ChannelStart(dbName,1)
	pluginObj.DBStart(dbName)


	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGINT)
	for sig := range signals {
		if sig == nil{
			continue
		}
		pluginObj.DelTableToServer(dbName,*schema_name,*table_name,httpServerKey,httpToServerId)

		pluginObj.DelTableToServer(dbName,*schema_name,*table_name,MongoDBServerKey,MongoDBKeyToServerId)
		pluginObj.DelTable(dbName,*schema_name,*table_name)

		pluginObj.ChannelStop(dbName,1)
		pluginObj.ChannelClose(dbName,1)
		pluginObj.ChannelDel(dbName,1)

		pluginObj.DBStop(dbName)
		pluginObj.DBClose(dbName)
		time.Sleep(1* time.Second)
		pluginObj.DBDel(dbName)

		pluginObj.DelToServer(MongoDBServerKey)

		pluginObj.DelToServer(httpServerKey)

		if result.insert == true{
			log.Println("http insert test success")
		}else{
			log.Println("http insert test failed")
		}

		if result.update == true{
			log.Println("http update test success")
		}else{
			log.Println("http update test failed")
		}

		if result.delete == true{
			log.Println("http delete test success")
		}else{
			log.Println("http delete test failed")
		}

		if result.query == true{
			log.Println("http query test success")
		}else{
			log.Println("http query test failed")
		}



		if resultKey.insert == true{
			log.Println("MongoDB key insert test success")
		}else{
			log.Println("MongoDB key insert test failed")
		}

		if resultKey.update == true{
			log.Println("MongoDB key update test success")
		}else{
			log.Println("MongoDB key update test failed")
		}

		if resultKey.delete == true{
			log.Println("MongoDB key delete test success")
		}else{
			log.Println("MongoDB key delete test failed")
		}

		if resultKey.query == true{
			log.Println("MongoDB key query test success")
		}else{
			log.Println("MongoDB key query test failed")
		}


		os.Exit(0)
	}
}

func initMongoDBConn(){
	var err error
	MongoDBClient,err =  mgo.Dial(*MongoDBServer)
	if err != nil{
		log.Println("initMongoDBConn err:",err)
		runtime.Goexit()
	}
}

func MongoDBSelect(where bson.M) map[string]interface{}{
	db := MongoDBClient.DB(*schema_name)	 //数据库名称
	collection := db.C(*table_name)
	var data map[string]interface{}
	err := collection.Find(where).One(&data)
	if err != nil{
		log.Println("MongoDBSelect where:",where," err",err)
		return nil
	}
	return data
}

func insertSQL(){
	insertSQL := "insert  into "+*schema_name+".`"+*table_name+"`(`id`,`testtinyint`,`testsmallint`,`testmediumint`,`testint`,`testbigint`,`testvarchar`,`testchar`,`testenum`,`testset`,`testtime`,`testdate`,`testyear`,`testtimestamp`,`testdatetime`,`testfloat`,`testdouble`,`testdecimal`,`testtext`,`testblob`,`testbit`,`testbool`,`testmediumblob`,`testlongblob`,`testtinyblob`,`test_unsinged_tinyint`,`test_unsinged_smallint`,`test_unsinged_mediumint`,`test_unsinged_int`,`test_unsinged_bigint`) values (1,-1,-2,-3,-4,-5,'testvarcha','te','en2','set1,set3','15:39:59','2018-05-08',2018,'2018-05-08 15:30:21','2018-05-08 15:30:21',9.39,9.39,9.39,'testtext','testblob','',1,'testmediumblob','testlongblob','testtinyblob',1,2,3,4,5)"
	pluginObj.MysqlConn.ExecSQL(insertSQL)
}

func updateSQL(){
	updateSQL := "update "+*schema_name+".`"+*table_name+"` set testvarchar = 'mytest',testbit=10 where id = 1"
	pluginObj.MysqlConn.ExecSQL(updateSQL)
}

func deleteSQL(){
	deleteSQL := "delete from "+*schema_name+".`"+*table_name+"` where id = 1"
	pluginObj.MysqlConn.ExecSQL(deleteSQL)
}

func alterSQL(){
	pluginObj.MysqlConn.ExecSQL("use "+*schema_name)
	ddlSQL := "ALTER TABLE `"+*schema_name+"`.`"+*table_name+"` CHANGE COLUMN `testvarchar` `testvarchar` varchar(20) NOT NULL"
	pluginObj.MysqlConn.ExecSQL(ddlSQL)
}


func handel_data(w http.ResponseWriter,req *http.Request){
	switch req.Method {
	case "GET":
		check_uri()
		break
	case "POST":
		post(w,req)
		break
	default:
		log.Println("Methon:",req.Method," not supported ")
		break
	}
	w.Write([]byte("success"))
}

func check_uri()  {
	log.Println("check uri success")
	return
}

func post(w http.ResponseWriter,req *http.Request)  {
	req.ParseForm()
	log.Println("EventType",req.Form.Get("EventType"))
	log.Println("SchemaName",req.Form.Get("SchemaName"))
	log.Println("TableName",req.Form.Get("TableName"))
	var err error
	switch req.Form.Get("EventType") {
	case "insert":
		err = insert(req.Form.Get("data"))
		break
	case "update":
		err = update(req.Form.Get("data"))
		break
	case "delete":
		err = delete(req.Form.Get("data"))
		break
	case "sql":
		err = query(req.Form.Get("data"))
		break
	default:
		log.Println("Data",req.Form.Get("data"))
		break
	}

	if err != nil{
		log.Println(err)
	}
	return
}

func insert(c string) error{
	var data map[string]interface{}
	err := json.Unmarshal([]byte(c),&data)
	if err != nil{
		return err
	}
	log.Println(data)

	result.insert = true
	time.Sleep(1*time.Second)

	m := make(bson.M,1)
	m["id"] = 1
	content := MongoDBSelect(m)
	if content != nil{
		log.Println("MongoDB insert get id=1"," value:",content)
		resultKey.insert = true
	}

	updateSQL()

	return nil
}

func update(c string) error{
	var data []map[string]interface{}
	err := json.Unmarshal([]byte(c),&data)
	if err != nil{
		return err
	}
	for k,v := range data[0]{
		log.Println(k,"before:",v, "after:",data[1][k])
	}
	if data[1]["testbit"].(float64) == 10 && data[1]["testvarchar"].(string) == "mytest"{
		result.update = true
	}
	time.Sleep(1*time.Second)

	m := make(bson.M,1)
	m["id"] = 1
	content := MongoDBSelect(m)
	if content != nil {
		log.Println("MongoDB update get id=1"," value:",content)
		if content["testbit"].(int64) == 10 && content["testvarchar"].(string) == "mytest" {
			resultKey.update = true
		}
	}

	deleteSQL()

	return nil
}

func delete(c string) error{
	var data map[string]interface{}
	err := json.Unmarshal([]byte(c),&data)
	if err != nil{
		return err
	}
	result.delete = true
	log.Println(data)

	time.Sleep(1*time.Second)

	m := make(bson.M,1)
	m["id"] = 1
	content := MongoDBSelect(m)
	if content == nil{
		log.Println("MongoDB delete get id=1"," value:",content)
		resultKey.delete = true
	}

	alterSQL()

	return nil
}

func query(c string) error{
	log.Println("data:",c)
	result.query = true
	return nil
}


func httpServer(ipAndPort string)  {
	http.HandleFunc(http_api,handel_data)
	http.ListenAndServe(ipAndPort, nil)
}