package main

import "time"
import (
	"flag"
	"log"
	"github.com/brokercap/Bifrost/test/pluginTest"
	"strconv"
	"os"
	"syscall"
	"os/signal"
	"encoding/json"
	"runtime"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"github.com/Shopify/sarama"
	"strings"
	"sync"
)

func init() {}


var schema_name *string
var table_name *string
var fieldList []string = make([]string,0)

type resultStruct struct {
	insert bool
	update bool
	delete bool
	query bool
}

var result *resultStruct

var b bool
var kafkaToServerId int

var pluginObj *pluginTest.BifrostManager
var kafkaServer *string
var kafkaClient sarama.Consumer

var Topic string

func main(){
	result = &resultStruct{
		false,false,false,false,
	}


	Topic = "bifrost_test_topic"

	bifrost_url := flag.String("bifrost_url", "http://127.0.0.1:21036", "-bifrost_url")
	bifrost_user := flag.String("bifrost_user", "Bifrost", "-bifrost_user")
	bifrost_pwd := flag.String("bifrost_pwd", "Bifrost123", "-bifrost_pwd")
	table_name = flag.String("table", "binlog_field_kafka_plugin_test", "-table")
	schema_name = flag.String("schema", "bifrost_test", "-schema")
	kafkaServer = flag.String("kafkaServer", "10.40.6.151:9092,10.40.6.152:9092", "-kafkaServer")
	DDL := flag.String("ddl", "true", "-ddl")

	mysqluser := flag.String("mysqluser", "root", "-mysqluser")
	mysqlpwd := flag.String("mysqlpwd", "", "-mysqlpwd")
	mysqlhost := flag.String("mysqlhost", "127.0.0.1", "-mysqlhost")
	mysqlport := flag.String("mysqlport", "3306", "-mysqlport")
	mysqldb := flag.String("mysqldb", "test", "-mysqldb")
	flag.Parse()

	dbSourceString := *mysqluser+":"+*mysqlpwd+"@tcp("+*mysqlhost+":"+*mysqlport+")/"+*mysqldb

	var dbName = "kafkaTest_"+strconv.FormatInt(time.Now().Unix(),10)

	var(
		kafkaServerKey string = "kafkaToserverTest_111111"
		kafkaPluginPamram map[string]interface{} = make(map[string]interface{},0)
	)

	kafkaPluginPamram["Topic"] = Topic
	kafkaPluginPamram["Key"] = ""
	kafkaPluginPamram["BatchSize"] = 10

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


	pluginObj.Init()

	pluginObj.AddToServer(kafkaServerKey,"kafka",*kafkaServer,kafkaServerKey)

	if *DDL == "true" {
		for _, sql := range sqlList {
			pluginObj.MysqlConn.ExecSQL(sql)
		}
	}

	go kafkaConsume()

	pluginObj.AddDB(dbName,*bifrost_url)
	pluginObj.AddTable(dbName,*schema_name,*table_name,1)

	b,kafkaToServerId= pluginObj.AddTableToServer(dbName,*schema_name,*table_name,kafkaServerKey,"kafka",fieldList,1,kafkaPluginPamram)
	if b == false{
		log.Println(dbName,*schema_name,*table_name,"add kafka toserver:",kafkaServerKey,false)
		runtime.Goexit()
	}


	insertSQL()
	updateSQL()
	deleteSQL()
	alterSQL()

	pluginObj.ChannelStart(dbName,1)
	pluginObj.DBStart(dbName)


	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGINT)
	for sig := range signals {
		if sig == nil{
			continue
		}

		pluginObj.DelTableToServer(dbName,*schema_name,*table_name,kafkaServerKey,kafkaToServerId)
		pluginObj.DelTable(dbName,*schema_name,*table_name)

		pluginObj.ChannelStop(dbName,1)
		pluginObj.ChannelClose(dbName,1)
		pluginObj.ChannelDel(dbName,1)

		pluginObj.DBStop(dbName)
		pluginObj.DBClose(dbName)
		time.Sleep(1* time.Second)
		pluginObj.DBDel(dbName)

		pluginObj.DelToServer(kafkaServerKey)

		if result.insert == true{
			log.Println("kafka insert test success")
		}else{
			log.Println("kafka insert test failed")
		}

		if result.update == true{
			log.Println("kafka update test success")
		}else{
			log.Println("kafka update test failed")
		}

		if result.delete == true{
			log.Println("kafka delete test success")
		}else{
			log.Println("kafka delete test failed")
		}

		if result.query == true{
			log.Println("kafka query test success")
		}else{
			log.Println("kafka query test failed")
		}

		os.Exit(0)
	}
}


func kafkaConsume(){
	var err error
	type kafkaClientStruct struct{
		client sarama.Consumer
		topic string
		wg sync.WaitGroup
	}
	var kafkaClient kafkaClientStruct
	var Consumer sarama.Consumer
	Consumer,err = sarama.NewConsumer(strings.Split(*kafkaServer,","),nil)
	kafkaClient = kafkaClientStruct{client:Consumer,topic:Topic}
	if err != nil{
		log.Println("kafkaConsume conn kafka err:",err)
		runtime.Goexit()
	}


	var myData pluginDriver.PluginDataType

	partitionList,err := kafkaClient.client.Partitions(kafkaClient.topic)
	if err != nil {
		log.Println("kafkaClient Partitions err:",err)
		runtime.Goexit()
		return
	}

	for partition := range partitionList {
		pc, errRet := kafkaClient.client.ConsumePartition(kafkaClient.topic,int32(partition),sarama.OffsetNewest)
		if errRet != nil {
			err = errRet
			log.Println("kafkaClient start consumer ,partition", partition," err:",err)
			runtime.Goexit()
			return
		}
		defer pc.AsyncClose()
		kafkaClient.wg.Add(1)
		go func(pc sarama.PartitionConsumer){
			for msg := range pc.Messages() {
				err := json.Unmarshal(msg.Value,&myData)
				if err != nil{
					log.Println("kafka consumer err:",string(msg.Value))
					runtime.Goexit()
				}
				switch myData.EventType{
				case "insert":
					result.insert = true
					break
				case "update":
					result.update = true
					break
				case "delete":
					result.delete = true
					break
				case "sql":
					result.query = true
					break
				default:
					log.Println("kafka err data")
					break
				}
				log.Println("kafka data:",myData)
			}
			kafkaClient.wg.Done()
		}(pc)
	}
	kafkaClient.wg.Wait()
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
