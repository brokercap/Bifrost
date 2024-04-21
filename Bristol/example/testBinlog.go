package main

import (
	"github.com/brokercap/Bifrost/Bristol/mysql"
	"log"
	"os"
	"reflect"
	"time"
)

func callback(data *mysql.EventReslut) {
	log.Println(data)
	if data.Query == "" {
		for k, v := range data.Rows[len(data.Rows)-1] {
			log.Println(k, "==", v, "(", reflect.TypeOf(v), ")")
		}
	}
}

func main() {
	//Position()
	//Gtid()
	Gtid2()
}

func Position() {
	filename := "mysql-bin.000071"

	var position uint32 = 203785789
	var DBsource = ""

	DBsource = "root:123456@tcp(192.168.126.128:3326)/bifrost_test"
	filename = "mysql-bin.000004"
	position = 3701

	reslut := make(chan error, 1)

	BinlogDump := &mysql.BinlogDump{
		DataSource:  DBsource,
		CallbackFun: callback,
		OnlyEvent:   []mysql.EventType{mysql.QUERY_EVENT, mysql.WRITE_ROWS_EVENTv1, mysql.UPDATE_ROWS_EVENTv1, mysql.DELETE_ROWS_EVENTv1, mysql.WRITE_ROWS_EVENTv2, mysql.UPDATE_ROWS_EVENTv2, mysql.DELETE_ROWS_EVENTv2},
	}
	BinlogDump.AddReplicateDoDb("bifrost_test", "*")
	BinlogDump.AddReplicateDoDb("test", "*")
	go BinlogDump.StartDumpBinlog(filename, position, 633, reslut, "", 0)
	go func() {
		for {
			v := <-reslut
			log.Printf("monitor reslut:%s \r\n", v)
			switch v.Error() {
			case "running", "starting":
				continue
			default:
				os.Exit(1)
			}
		}
	}()

	for {
		time.Sleep(10 * time.Second)
	}
}

func Gtid() {

	var DBsource = ""
	var gtid = ""

	DBsource = "root:root@tcp(192.168.220.128:3356)/bifrost_test"

	gtid = "0-10-7196"

	reslut := make(chan error, 1)
	BinlogDump := &mysql.BinlogDump{
		DataSource:  DBsource,
		CallbackFun: callback,
		OnlyEvent:   []mysql.EventType{mysql.QUERY_EVENT, mysql.WRITE_ROWS_EVENTv1, mysql.UPDATE_ROWS_EVENTv1, mysql.DELETE_ROWS_EVENTv1, mysql.WRITE_ROWS_EVENTv2, mysql.UPDATE_ROWS_EVENTv2, mysql.DELETE_ROWS_EVENTv2},
	}
	BinlogDump.AddReplicateDoDb("bifrost_test", "*")
	BinlogDump.AddReplicateDoDb("test", "*")
	go BinlogDump.StartDumpBinlogGtid(gtid, 633, reslut)
	go func() {
		for {
			v := <-reslut
			log.Printf("monitor reslut:%s \r\n", v)
		}
	}()

	time.Sleep(20 * time.Second)
	return

}

func Gtid2() {

	var DBsource = ""
	var gtid = ""

	DBsource = "root:root@tcp(172.17.0.3:3306)/bifrost_test"

	gtid = "03b14c99-63fb-11eb-9902-0242ac110003:1-9,41f084ba-63f4-11eb-96a1-0242ac110002:1-12"

	reslut := make(chan error, 1)
	BinlogDump := &mysql.BinlogDump{
		DataSource:  DBsource,
		CallbackFun: callback,
		OnlyEvent:   []mysql.EventType{mysql.QUERY_EVENT, mysql.WRITE_ROWS_EVENTv1, mysql.UPDATE_ROWS_EVENTv1, mysql.DELETE_ROWS_EVENTv1, mysql.WRITE_ROWS_EVENTv2, mysql.UPDATE_ROWS_EVENTv2, mysql.DELETE_ROWS_EVENTv2},
	}
	BinlogDump.AddReplicateDoDb("bifrost_test", "*")
	BinlogDump.AddReplicateDoDb("test", "*")
	go BinlogDump.StartDumpBinlogGtid(gtid, 633, reslut)
	go func() {
		for {
			v := <-reslut
			log.Printf("monitor reslut:%s \r\n", v)
		}
	}()

	time.Sleep(20 * time.Second)
	return

}
