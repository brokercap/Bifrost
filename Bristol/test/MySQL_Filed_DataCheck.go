package main

import (
	"log"
	"github.com/brokercap/Bifrost/Bristol/mysql"
	"time"
	"os"
	"fmt"
	"reflect"
	"strconv"
	"database/sql/driver"
	"flag"
)

func DBConnect(uri string) mysql.MysqlConnection{
	db := mysql.NewConnect(uri)
	return db
}

type MasterBinlogInfoStruct struct {
	File string
	Position int
	Binlog_Do_DB string
	Binlog_Ignore_DB string
	Executed_Gtid_Set string
}

func GetBinLogInfo(db mysql.MysqlConnection) MasterBinlogInfoStruct{
	sql := "SHOW MASTER STATUS"
	stmt,err := db.Prepare(sql)
	if err !=nil{
		log.Println(err)
		return MasterBinlogInfoStruct{}
	}
	defer stmt.Close()
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	defer rows.Close()
	if err != nil {
		log.Printf("%v\n", err)
		return MasterBinlogInfoStruct{}
	}
	var File string
	var Position int
	var Binlog_Do_DB string
	var Binlog_Ignore_DB string
	var Executed_Gtid_Set string
	for {
		dest := make([]driver.Value, 4, 4)
		errs := rows.Next(dest)
		if errs != nil {
			return MasterBinlogInfoStruct{}
		}
		File = string(dest[0].([]byte))
		Binlog_Do_DB = string(dest[2].([]byte))
		Binlog_Ignore_DB = string(dest[3].([]byte))
		Executed_Gtid_Set = ""
		PositonString := string(dest[1].([]byte))
		Position,_ = strconv.Atoi(PositonString)
		break
	}

	return MasterBinlogInfoStruct{
		File:File,
		Position:Position,
		Binlog_Do_DB:Binlog_Do_DB,
		Binlog_Ignore_DB:Binlog_Ignore_DB,
		Executed_Gtid_Set:Executed_Gtid_Set,
	}
}

func GetServerId(db mysql.MysqlConnection) int{
	sql := "show variables like 'server_id'"
	stmt,err := db.Prepare(sql)
	if err !=nil{
		log.Println(err)
		return 0
	}
	defer stmt.Close()
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	if err != nil {
		log.Printf("%v\n", err)
		return 0
	}
	defer rows.Close()
	var ServerId int
	for{
		dest := make([]driver.Value, 2, 2)
		errs := rows.Next(dest)
		if errs != nil{
			return 0
		}
		ServerIdString := string(dest[1].([]byte))
		ServerId,_ = strconv.Atoi(ServerIdString)
		break
	}
	return ServerId
}

func ExecSQL(db mysql.MysqlConnection,sql string){
	p := make([]driver.Value, 0)
	db.Exec(sql,p)
	return
}

var sqlList = []string{
	"CREATE DATABASE /*!32312 IF NOT EXISTS*/ `bifrost_test`",
	"DROP TABLE IF EXISTS bifrost_test.`binlog_field_test`",
	"CREATE TABLE bifrost_test.`binlog_field_test` ("+
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
  	"insert  into bifrost_test.`binlog_field_test`(`id`,`testtinyint`,`testsmallint`,`testmediumint`,`testint`,`testbigint`,`testvarchar`,`testchar`,`testenum`,`testset`,`testtime`,`testdate`,`testyear`,`testtimestamp`,`testdatetime`,`testfloat`,`testdouble`,`testdecimal`,`testtext`,`testblob`,`testbit`,`testbool`,`testmediumblob`,`testlongblob`,`testtinyblob`,`test_unsinged_tinyint`,`test_unsinged_smallint`,`test_unsinged_mediumint`,`test_unsinged_int`,`test_unsinged_bigint`) values (1,-1,-2,-3,-4,-5,'testvarcha','te','en2','set1,set3','15:39:59','2018-05-08',2018,'2018-05-08 15:30:21','2018-05-08 15:30:21',9.39,9.39,9.39,'testtext','testblob','',1,'testmediumblob','testlongblob','testtinyblob',1,2,3,4,5)",
}

func evenTypeName(e mysql.EventType) string {
	switch e {
	case mysql.WRITE_ROWS_EVENTv0, mysql.WRITE_ROWS_EVENTv1, mysql.WRITE_ROWS_EVENTv2:
		return "insert"
	case mysql.UPDATE_ROWS_EVENTv0, mysql.UPDATE_ROWS_EVENTv1, mysql.UPDATE_ROWS_EVENTv2:
		return "update"
	case mysql.DELETE_ROWS_EVENTv0, mysql.DELETE_ROWS_EVENTv1, mysql.DELETE_ROWS_EVENTv2:
		return "delete"
	}
	return fmt.Sprintf("%d", e)
}

func callback2(d *mysql.EventReslut) {
	fmt.Println("schema:",d.SchemaName,"table:",d.TableName, "EventType:",evenTypeName(d.Header.EventType))

	if d.Header.EventType == mysql.QUERY_EVENT{
		log.Println(d.Query)
		return
	}
	index := 0
	if len(d.Rows) > 1{
		index = 1
	}
	data := d.Rows[index]
	if d.SchemaName == "bifrost_test" && d.TableName == "binlog_field_test"{

	}else{
		for k,v := range data{
			log.Println(k,":",v)
		}
		return
	}
	noError := true
	for k,v := range data{
		switch k {
		case "id":
			switch v.(type) {
			case uint32:
				if v.(uint32) != 1{
					log.Println(k,1,"!=",v)
					noError  = false
				}else{
					log.Println(k,1,"==",v,"filed-Type:","uint","golang-type:",reflect.TypeOf(v)," is right")
				}
				break
			default:
				log.Println(k,1,"!=",v, " type:",reflect.TypeOf(v))
				noError  = false
			}
			break
		case "testtinyint":
			switch v.(type) {
			case int8:
				if v.(int8) != -1{
					log.Println(k,-1,"!=",v)
					noError  = false
				}else{
					log.Println(k,1,"==",v,"filed-Type:","tinyint","golang-type:",reflect.TypeOf(v)," is right")
				}
				break
			default:
				log.Println(k,-1,"!=",v, " type:",reflect.TypeOf(v))
				noError  = false
			}
			break
		case "testsmallint":
			switch v.(type) {
			case int16:
				if v.(int16) != -2{
					log.Println(k,-2,"!=",v)
					noError  = false
				}else{
					log.Println(k,-2,"==",v,"filed-Type:","smallint","golang-type:",reflect.TypeOf(v)," is right")
				}
				break
			default:
				log.Println(k,-3,"!=",v, " type:",reflect.TypeOf(v))
				noError  = false
			}
			break
		case "testmediumint":
			switch v.(type) {
			case int32:
				if v.(int32) != -3{
					log.Println(k,-3,"!=",v)
					noError  = false
				}else{
					log.Println(k,-3,"==",v,"filed-Type:","mediumint","golang-type:",reflect.TypeOf(v)," is right")
				}
				break
			default:
				log.Println(k,-3,"!=",v, " type:",reflect.TypeOf(v))
				noError  = false
			}
			break
		case "testint":
			switch v.(type) {
			case int32:
				if v.(int32) != -4{
					log.Println(k,-4,"!=",v)
					noError  = false
				}else{
					log.Println(k,-4,"==",v,"filed-Type:","int","golang-type:",reflect.TypeOf(v)," is right")
				}
				break
			default:
				log.Println(k,-4,"!=",v, " type:",reflect.TypeOf(v))
				noError  = false
			}
			break
		case "testbigint":
			switch v.(type) {
			case int64:
				if v.(int64) != -5{
					log.Println(k,-5,"!=",v)
					noError  = false
				}else{
					log.Println(k,-5,"==",v,"filed-Type:","bigint","golang-type:",reflect.TypeOf(v)," is right")
				}
				break
			default:
				log.Println(k,-5,"!=",v, " type:",reflect.TypeOf(v))
				noError  = false
			}
			break

		case "testvarchar":
			if v.(string) != "testvarcha"{
				log.Println(k,"testvarcha","!=",v)
				noError  = false
			}else{
				log.Println(k,"testvarcha","==",v,"filed-Type:","varchar","golang-type:",reflect.TypeOf(v)," is right")
			}

			break
		case "testchar":
			if v.(string) != "te"{
				log.Println(k,"te","!=",v)
				noError  = false
			}else{
				log.Println(k,"te","==",v,"filed-Type:","char","golang-type:",reflect.TypeOf(v)," is right")
			}
			break
		case "testenum":
			if v.(string) != "en2"{
				log.Println(k,"te","!=",v)
				noError  = false
			}else{
				log.Println(k,"en2","==",v,"filed-Type:","enum","golang-type:",reflect.TypeOf(v)," is right")
			}
			break
		case "testset":
			f := v.([]string)
			var b bool = true
			if f[0] != "set1" && f[1] != "set1"{
				log.Println(k,"set1 no exsit",f)
				noError  = false
				b = false
			}
			if f[1] != "set3" && f[0] != "set3"{
				log.Println(k,"set3 no exsit",f)
				noError  = false
				b = false
			}
			if b == true{
				log.Println(k,"(set1,set3)","==",v,"filed-Type:","set","golang-type:",reflect.TypeOf(v)," is right")
			}
			break
		case "testtime":
			if v.(string) != "15:39:59"{
				log.Println(k,"15:39:59","!=",v)
				noError  = false
			}else{
				log.Println(k,"15:39:59","==",v,"filed-Type:","time","golang-type:",reflect.TypeOf(v)," is right")
			}
			break
		case "testdate":
			if v.(string) != "2018-05-08"{
				log.Println(k,"2018-05-08","!=",v)
				noError  = false
			}else{
				log.Println(k,"2018-05-08","==",v,"filed-Type:","date","golang-type:",reflect.TypeOf(v)," is right")
			}
			break

		case "testyear":
			if v.(string) != "2018"{
				log.Println(k,"2018","!=",v)
				noError  = false
			}else{
				log.Println(k,"2018","==",v,"filed-Type:","year","golang-type:",reflect.TypeOf(v)," is right")
			}
			break
		case "testtimestamp":
			if v.(string) != "2018-05-08 15:30:21"{
				log.Println(k,"2018-05-08 15:30:21","!=",v)
				noError  = false
			}else{
				log.Println(k,"2018-05-08 15:30:21","==",v,"filed-Type:","timestamp","golang-type:",reflect.TypeOf(v)," is right")
			}
			break
		case "testdatetime":
			if v.(string) != "2018-05-08 15:30:21"{
				log.Println(k,"2018-05-08 15:30:21","!=",v)
				noError  = false
			}else{
				log.Println(k,"2018-05-08 15:30:21","==",v,"filed-Type:","datetime","golang-type:",reflect.TypeOf(v)," is right")
			}
			break
		case "testfloat":
			if v.(float32) != 9.39{
				log.Println(k,9.39,"!=",v)
				noError  = false
			}else{
				log.Println(k,9.39,"==",v,"filed-Type:","float","golang-type:",reflect.TypeOf(v)," is right")
			}
			break
		case "testdouble":
			if v.(float64) != 9.39{
				log.Println(k,9.39,"!=",v)
				noError  = false
			}else{
				log.Println(k,9.39,"==",v,"filed-Type:","double","golang-type:",reflect.TypeOf(v)," is right")
			}
			break

		case "testdecimal":
			if v.(string) != "9.39"{
				log.Println(k,9.39,"!=",v)
				noError  = false
			}else{
				log.Println(k,9.39,"==",v,"filed-Type:","decimal","golang-type:",reflect.TypeOf(v)," is right")
			}
			break

		case "testtext":
			if v.(string) != "testtext"{
				log.Println(k,"testtext","!=",v)
				noError  = false
			}else{
				log.Println(k,"testtext","==",v,"filed-Type:","text","golang-type:",reflect.TypeOf(v)," is right")
			}
			break

		case "testblob":
			if v.(string) != "testblob"{
				log.Println(k,"testblob","!=",v)
				noError  = false
			}else{
				log.Println(k,"testblob","==",v,"filed-Type:","blob","golang-type:",reflect.TypeOf(v)," is right")
			}
			break

		case "testbit":
			switch v.(type) {
			case int64:
				if v.(int64) != 8{
					log.Println(k,8,"!=",v)
					noError  = false
				}else{
					log.Println(k,"8","==",v,"filed-Type:","bit","golang-type:",reflect.TypeOf(v)," is right")
				}
				break
			default:
				log.Println(k,8,"!=",v, " type:",reflect.TypeOf(v))
				noError  = false
			}
			break

		case "testbool":

			switch v.(type) {
			case bool:
				if v.(bool) != true{
					log.Println(k,"true","!=",v)
					noError  = false
				}else{
					log.Println(k,"true","==",v,"filed-Type:","bool","golang-type:",reflect.TypeOf(v)," is right")
				}
				break
			default:
				log.Println(k,"true","!=",v, " type:",reflect.TypeOf(v))
				noError  = false
			}
			break

		case "testmediumblob":
			if v.(string) != "testmediumblob"{
				log.Println(k,"testmediumblob","!=",v)
				noError  = false
			}else{
				log.Println(k,"testmediumblob","==",v,"filed-Type:","mediumblob","golang-type:",reflect.TypeOf(v)," is right")
			}
			break

		case "testlongblob":
			if v.(string) != "testlongblob"{
				log.Println(k,"testlongblob","!=",v)
				noError  = false
			}else{
				log.Println(k,"testlongblob","==",v,"filed-Type:","longblob","golang-type:",reflect.TypeOf(v)," is right")
			}
			break

		case "testtinyblob":
			if v.(string) != "testtinyblob"{
				log.Println(k,"testtinyblob","!=",v)
				noError  = false
			}else{
				log.Println(k,"testtinyblob","==",v,"filed-Type:","tinyblob","golang-type:",reflect.TypeOf(v)," is right")
			}
			break

		case "test_unsinged_tinyint":
			switch v.(type) {
			case uint8:
				if v.(uint8) != 1{
					log.Println(k,1,"!=",v)
					noError  = false
				}else{
					log.Println(k,"1","==",v,"filed-Type:","unsinged_tinyint","golang-type:",reflect.TypeOf(v)," is right")
				}
				break
			default:
				log.Println(k,1,"!=",v, " type:",reflect.TypeOf(v))
				noError  = false
			}

			break

		case "test_unsinged_smallint":
			switch v.(type) {
			case uint16:
				if v.(uint16) != 2{
					log.Println(k,2,"!=",v)
					noError  = false
				}else{
					log.Println(k,"2","==",v,"filed-Type:","unsinged_smallint","golang-type:",reflect.TypeOf(v)," is right")
				}
				break
			default:
				log.Println(k,2,"!=",v, " type:",reflect.TypeOf(v))
				noError  = false
			}
			break

		case "test_unsinged_mediumint":
			switch v.(type) {
			case uint32:
				if v.(uint32) != 3{
					log.Println(k,3,"!=",v)
					noError  = false
				}else{
					log.Println(k,"3","==",v,"filed-Type:","unsinged_mediumint","golang-type:",reflect.TypeOf(v)," is right")
				}
				break
			default:
				log.Println(k,3,"!=",v, " type:",reflect.TypeOf(v))
				noError  = false
			}
			break

		case "test_unsinged_int":
			switch v.(type) {
			case uint32:
				if v.(uint32) != 4{
					log.Println(k,4,"!=",v)
					noError  = false
				}else{
					log.Println(k,"4","==",v,"filed-Type:","unsinged_int","golang-type:",reflect.TypeOf(v)," is right")
				}
				break
			default:
				log.Println(k,4,"!=",v, " type:",reflect.TypeOf(v))
				noError  = false
			}
			break

		case "test_unsinged_bigint":
			switch v.(type) {
			case uint64:
				if v.(uint64) != 5{
					log.Println(k,5,"!=",v)
					noError  = false
				}else{
					log.Println(k,"5","==",v,"filed-Type:","unsinged_bigint","golang-type:",reflect.TypeOf(v)," is right")
				}
				break
			default:
				log.Println(k,5,"!=",v, " type:",reflect.TypeOf(v))
				noError  = false
			}
			break
		default:
			fmt.Println(k,":",v," error type")
			noError  = false
		}
	}
	if noError  == true{
		log.Println(" type and value is all right ")
	}
	os.Exit(0)
}

func main() {

	userName := flag.String("u", "root", "-u root")
	password := flag.String("p", "", "-p password")
	host := flag.String("h", "127.0.0.1", "-h 127.0.0.1")
	port := flag.String("P", "3306", "-P 3306")
	database := flag.String("database", "test", "-database test")
	flag.Parse()

	var filename,dataSource string
	var position uint32 = 0
	var MyServerID uint32 = 0

	dataSource = *userName+":"+*password+"@tcp("+*host+":"+*port+")/"+*database
	log.Println(dataSource," start connect")
	db := DBConnect(dataSource)
	if db == nil {
		log.Println("dataSource:",dataSource," connect err")
		return
	}
	log.Println(dataSource," start success")
	masterInfo := GetBinLogInfo(db)
	if masterInfo.File == ""{
		log.Println(dataSource," binlog disabled")
		os.Exit(0)
	}

	filename = masterInfo.File;
	position = uint32(masterInfo.Position)
	masterServerId := GetServerId(db)
	MyServerID = uint32(masterServerId+250)

	log.Println("load data start")
	for _,sql := range  sqlList{
		log.Println("exec sql:",sql)
		ExecSQL(db,sql)
	}
	log.Println("load data over")

	reslut := make(chan error, 1)
	m := make(map[string]uint8, 0)
	m["bifrost_test"] = 1
	BinlogDump := &mysql.BinlogDump{
		DataSource:    dataSource,
		CallbackFun:   callback2,
		ReplicateDoDb: m,
		OnlyEvent:     []mysql.EventType{
			mysql.QUERY_EVENT,
			mysql.WRITE_ROWS_EVENTv1, mysql.UPDATE_ROWS_EVENTv1, mysql.DELETE_ROWS_EVENTv1,
			mysql.WRITE_ROWS_EVENTv0, mysql.UPDATE_ROWS_EVENTv0, mysql.DELETE_ROWS_EVENTv0,
			mysql.WRITE_ROWS_EVENTv2, mysql.UPDATE_ROWS_EVENTv2, mysql.DELETE_ROWS_EVENTv2,
		},
	}
	go BinlogDump.StartDumpBinlog(filename, position, MyServerID,reslut,"",0)
	go func() {
		for {
			v := <-reslut
			log.Printf("monitor reslut:%s \r\n", v)
			if v.Error() == "close" {
				os.Exit(1)
			}
		}
	}()
	for {
		time.Sleep(10 * time.Second)
	}
}
