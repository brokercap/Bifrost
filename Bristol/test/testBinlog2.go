package main

import (
	"log"

	"github.com/Bifrost/Bristol/mysql"
	"time"
	"os"
	"fmt"
	"reflect"
)

func main() {

	var filename,dataSource string
	var position uint32 = 120

	/** mysql 5.1.73*/
	filename = "mysql-bin.000001"
	position = 106
	dataSource = "root:root@tcp(10.40.6.232:3306)/test"


	/** mysql 5.6.36*/
	/*
	filename = "mysql-bin.000007"
	position = 2845
	dataSource = "root:root@tcp(127.0.0.1:3306)/bifrost_test"
	*/

	/** mysql 5.7.18*/
	/*
	filename = "mysql-bin.000007"
	position = 2928
	dataSource = "root:root@tcp(10.4.4.200:3306)/bifrost_test"
	*/


	/** mysql 5.6.23*/
	/*
	filename = "mysql-bin.000056"
	position = 1050557687
	dataSource = "root:root@tcp(10.40.6.89:3306)/bifrost_test"
	*/

	/** mysql 8.0.11*/
	/*
	filename = "binlog.000002"
	position = 1565
	dataSource = "sroot:Xiezuofu_123@tcp(10.4.4.199:3306)/bifrost_test"
	*/

	reslut := make(chan error, 1)
	m := make(map[string]uint8, 0)
	m["bifrost_test"] = 1
	BinlogDump := &mysql.BinlogDump{
		DataSource:    dataSource,
		CallbackFun:   callback2,
		ReplicateDoDb: m,
		OnlyEvent:     []mysql.EventType{mysql.QUERY_EVENT,mysql.WRITE_ROWS_EVENTv1, mysql.UPDATE_ROWS_EVENTv1, mysql.DELETE_ROWS_EVENTv1,mysql.WRITE_ROWS_EVENTv0, mysql.UPDATE_ROWS_EVENTv0, mysql.DELETE_ROWS_EVENTv0,mysql.WRITE_ROWS_EVENTv2, mysql.UPDATE_ROWS_EVENTv2, mysql.DELETE_ROWS_EVENTv2},
	}
	go BinlogDump.StartDumpBinlog(filename, position, 100,reslut,"",0)
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
	if d.SchemaName == "bifrost_test" && d.TableName == "test3"{

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
				}
				break
			default:
				log.Println(k,-3,"!=",v, " type:",reflect.TypeOf(v))
				noError  = false
			}
			break
		case "testmediumint":
			switch v.(type) {
			case int:
				if v.(int) != -3{
					log.Println(k,-3,"!=",v)
					noError  = false
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
			}
			break
		case "testchar":
			if v.(string) != "te"{
				log.Println(k,"te","!=",v)
				noError  = false
			}
			break
		case "testenum":
			if v.(string) != "en2"{
				log.Println(k,"te","!=",v)
				noError  = false
			}
			break
		case "testset":
			f := v.([]string)
			if f[0] != "set1" && f[1] != "set1"{
				log.Println(k,"set1 no exsit",f)
				noError  = false
			}
			if f[1] != "set3" && f[0] != "set3"{
				log.Println(k,"set3 no exsit",f)
				noError  = false
			}
			break
		case "testtime":
			if v.(string) != "15:39:59"{
				log.Println(k,"15:39:59","!=",v)
				noError  = false
			}
			break
		case "testdate":
			if v.(string) != "2018-05-08"{
				log.Println(k,"2018-05-08","!=",v)
				noError  = false
			}
			break

		case "testyear":
			if v.(string) != "2018"{
				log.Println(k,"2018","!=",v)
				noError  = false
			}
			break
		case "testtimestamp":
			if v.(string) != "2018-05-08 15:30:21"{
				log.Println(k,"2018-05-08 15:30:21","!=",v)
				noError  = false
			}
			break
		case "testdatetime":
			if v.(string) != "2018-05-08 15:30:21"{
				log.Println(k,"2018-05-08 15:30:21","!=",v)
				noError  = false
			}
			break
		case "testfloat":
			if v.(float32) != 9.39{
				log.Println(k,9.39,"!=",v)
				noError  = false
			}
			break
		case "testdouble":
			if v.(float64) != 9.39{
				log.Println(k,9.39,"!=",v)
				noError  = false
			}
			break

		case "testdecimal":
			if v.(string) != "9.39"{
				log.Println(k,9.39,"!=",v)
				noError  = false
			}
			break

		case "testtext":
			if v.(string) != "testtext"{
				log.Println(k,"testtext","!=",v)
				noError  = false
			}
			break

		case "testblob":
			if v.(string) != "testblob"{
				log.Println(k,"testblob","!=",v)
				noError  = false
			}
			break

		case "testbit":
			switch v.(type) {
			case int64:
				if v.(int64) != 8{
					log.Println(k,8,"!=",v)
					noError  = false
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
			}
			break

		case "testlongblob":
			if v.(string) != "testlongblob"{
				log.Println(k,"testlongblob","!=",v)
				noError  = false
			}
			break

		case "testtinyblob":
			if v.(string) != "testtinyblob"{
				log.Println(k,"testtinyblob","!=",v)
				noError  = false
			}
			break

		case "test_unsinged_tinyint":
			switch v.(type) {
			case uint8:
				if v.(uint8) != 1{
					log.Println(k,1,"!=",v)
					noError  = false
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
				}
				break
			default:
				log.Println(k,2,"!=",v, " type:",reflect.TypeOf(v))
				noError  = false
			}
			break

		case "test_unsinged_mediumint":
			switch v.(type) {
			case uint:
				if v.(uint) != 3{
					log.Println(k,3,"!=",v)
					noError  = false
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
}
