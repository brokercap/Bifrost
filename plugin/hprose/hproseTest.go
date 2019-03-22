package main

import "time"
import (
	"flag"
	"log"
	"github.com/jc3wish/Bifrost/test/pluginTest"
	"github.com/hprose/hprose-golang/rpc"
	"strconv"
	"os"
	"syscall"
	"os/signal"
	"fmt"
)

func init() {}


var schema_name string = "bifrost_test"
var table_name string = "binlog_field_hprose_plugin_test"
var fieldList []string = make([]string,0)

type resultStruct struct {
	insert bool
	update bool
	delete bool
	query bool
}

var result *resultStruct

func main(){
	result = &resultStruct{
		false,false,false,false,
	}

	bifrost_url := flag.String("bifrost_url", "http://127.0.0.1:21036", "-bifrost_url")
	bifrost_user := flag.String("bifrost_user", "Bifrost", "-bifrost_user")
	bifrost_pwd := flag.String("bifrost_pwd", "Bifrost123", "-bifrost_pwd")
	table_name := flag.String("table", "binlog_field_hprose_plugin_test", "-table")
	schema_name := flag.String("schema", "bifrost_test", "-schema")
	pluginServer := flag.String("pluginServer", "tcp4://127.0.0.1:4322/", "-pluginServer")
	DDL := flag.String("ddl", "true", "-ddl")

	mysqluser := flag.String("mysqluser", "root", "-mysqluser")
	mysqlpwd := flag.String("mysqlpwd", "", "-mysqlpwd")
	mysqlhost := flag.String("mysqlhost", "127.0.0.1", "-mysqlhost")
	mysqlport := flag.String("mysqlport", "3306", "-mysqlport")
	mysqldb := flag.String("mysqldb", "test", "-mysqldb")
	flag.Parse()

	dbSourceString := *mysqluser+":"+*mysqlpwd+"@tcp("+*mysqlhost+":"+*mysqlport+")/"+*mysqldb
	var dbName = "hposeTest_"+strconv.FormatInt(time.Now().Unix(),10)

	var(
		toServerKey string = "hproseToserverTest_111111"
		pluginPamram map[string]interface{} = make(map[string]interface{},0)
	)

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


	log.Println("start ",time.Now().Format("2006-01-02 15:04:05"))
	//forInsert(dbstring,*schema,*table,*count)
	log.Println("end ",time.Now().Format("2006-01-02 15:04:05"))
	pluginObj := pluginTest.BifrostManager{
		Host: *bifrost_url,
		User: *bifrost_user,
		Pwd:  *bifrost_pwd,
		MysqlConn: &pluginTest.MySQLConn{
			Uri: dbSourceString,
		},
	}

	pluginObj.Init()
	b := pluginObj.AddToServer(toServerKey,"hprose",*pluginServer,toServerKey)
	if b == false{
		log.Println("AddToServer false")
		os.Exit(1)
	}
	if *DDL == "true" {
		for _, sql := range sqlList {
			pluginObj.MysqlConn.ExecSQL(sql)
		}
	}

	go HproseServer(*pluginServer)

	pluginObj.AddDB(dbName,*bifrost_url)
	pluginObj.AddTable(dbName,*schema_name,*table_name,1)
	pluginObj.AddTableToServer(dbName,*schema_name,*table_name,toServerKey,"hprose",fieldList,1,pluginPamram)

	insertSQL := "insert  into "+*schema_name+".`"+*table_name+"`(`id`,`testtinyint`,`testsmallint`,`testmediumint`,`testint`,`testbigint`,`testvarchar`,`testchar`,`testenum`,`testset`,`testtime`,`testdate`,`testyear`,`testtimestamp`,`testdatetime`,`testfloat`,`testdouble`,`testdecimal`,`testtext`,`testblob`,`testbit`,`testbool`,`testmediumblob`,`testlongblob`,`testtinyblob`,`test_unsinged_tinyint`,`test_unsinged_smallint`,`test_unsinged_mediumint`,`test_unsinged_int`,`test_unsinged_bigint`) values (1,-1,-2,-3,-4,-5,'testvarcha','te','en2','set1,set3','15:39:59','2018-05-08',2018,'2018-05-08 15:30:21','2018-05-08 15:30:21',9.39,9.39,9.39,'testtext','testblob','',1,'testmediumblob','testlongblob','testtinyblob',1,2,3,4,5)"
	pluginObj.MysqlConn.ExecSQL(insertSQL)

	updateSQL := "update "+*schema_name+".`"+*table_name+"` set testvarchar = 'mytest',testbit=10 where id = 1"
	pluginObj.MysqlConn.ExecSQL(updateSQL)

	deleteSQL := "delete from "+*schema_name+".`"+*table_name+"` where id = 1"
	pluginObj.MysqlConn.ExecSQL(deleteSQL)

	ddlSQL := "ALTER TABLE `"+*schema_name+"`.`"+*table_name+"` CHANGE COLUMN `testvarchar` `testvarchar` varchar(20) NOT NULL"
	pluginObj.MysqlConn.ExecSQL(ddlSQL)

	pluginObj.ChannelStart(dbName,1)
	pluginObj.DBStart(dbName)


	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGINT)
	for sig := range signals {
		if sig == nil{
			continue
		}
		pluginObj.DelTableToServer(dbName,*schema_name,*table_name,toServerKey,1)
		pluginObj.DelTable(dbName,*schema_name,*table_name)

		pluginObj.ChannelStop(dbName,1)
		pluginObj.ChannelClose(dbName,1)
		pluginObj.ChannelDel(dbName,1)

		pluginObj.DBStop(dbName)
		pluginObj.DBClose(dbName)
		time.Sleep(1* time.Second)
		pluginObj.DBDel(dbName)

		pluginObj.DelToServer(toServerKey)

		if result.insert == true{
			log.Println("insert test success")
		}else{
			log.Println("insert test failed")
		}

		if result.update == true{
			log.Println("update test success")
		}else{
			log.Println("update test failed")
		}

		if result.delete == true{
			log.Println("delete test success")
		}else{
			log.Println("delete test failed")
		}

		if result.query == true{
			log.Println("query test success")
		}else{
			log.Println("query test failed")
		}
		os.Exit(0)
	}
}


func Check(context *rpc.HTTPContext) (e error) {
	log.Println("Check success")
	return nil
}

func Insert(SchemaName string,TableName string, data map[string]interface{}) (e error) {
	log.Println("Insert","SchemaName:",SchemaName,"TableName:",TableName,"data:",data)
	result.insert = true
	return nil
}

func Update(SchemaName string,TableName string, data []map[string]interface{}) (e error){
	log.Println("Update")
	log.Println("SchemaName:",SchemaName)
	log.Println("TableName:",TableName)
	log.Println("data:",data)
	log.Println("Update","SchemaName:",SchemaName,"TableName:",TableName)
	for k,v := range data[0]{
		log.Println(k,"before:",v, "after:",data[1][k])
	}
	if fmt.Sprint(data[1]["testbit"]) == "10" && data[1]["testvarchar"].(string) == "mytest"{
		result.update = true
	}
	return nil
}

func Delete(SchemaName string,TableName string,data map[string]interface{}) (e error) {
	log.Println("Delete","SchemaName:",SchemaName,"TableName:",TableName,"data:",data)
	result.delete = true
	return nil
}

func Query(SchemaName string,TableName string,data interface{}) (e error) {
	log.Println("Query","SchemaName:",SchemaName,"TableName:",TableName,"data:",data)
	result.query = true
	return nil
}

func HproseServer(pluginServer string){
	service := rpc.NewTCPServer(pluginServer)
	service.Debug = true
	service.AddFunction("Insert", Insert)
	service.AddFunction("Update", Update)
	service.AddFunction("Delete", Delete)
	service.AddFunction("Query", Query)
	service.AddFunction("Check", Check)
	service.Start()
}