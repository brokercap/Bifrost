package main

import (
	"log"

	"github.com/brokercap/Bifrost/Bristol/mysql"
	"reflect"
	"time"
)

func callback(data *mysql.EventReslut) {
	log.Println(data)
	if data.Query == ""{
		for k,v := range data.Rows[len(data.Rows)-1]{
			log.Println(k,"==",v,"(",reflect.TypeOf(v),")")
		}
	}
}

func main() {
	filename := "mysql-bin.000071"

	var position uint32 = 203785789
	var DBsource = ""

	DBsource = "root:root@tcp(192.168.220.128:3308)/bifrost_test"
	filename = "mysql-bin.000003"
	position = 339

	DBsource = "root:root@tcp(192.168.220.128:3307)/bifrost_test"
	filename = "mysql-bin.000016"
	position = 11857


	DBsource = "root:root@tcp(192.168.220.128:3308)/bifrost_test"
	filename = "mysql-bin.000004"
	position = 25051078

	DBsource = "root:root@tcp(192.168.220.147:3307)/bifrost_test"
	filename = "mysql-bin.000051"
	position = 2223

	DBsource = "root:root@tcp(192.168.220.147:3308)/bifrost_test"
	filename = "mysql-bin.000021"
	position = 2276

	reslut := make(chan error, 1)
	m := make(map[string]uint8, 0)
	m["bifrost_test"] = 1
	BinlogDump := &mysql.BinlogDump{
		DataSource:    DBsource,
		CallbackFun:   callback,
		//ReplicateDoDb: m,
		OnlyEvent:     []mysql.EventType{mysql.QUERY_EVENT, mysql.WRITE_ROWS_EVENTv1, mysql.UPDATE_ROWS_EVENTv1, mysql.DELETE_ROWS_EVENTv1,mysql.WRITE_ROWS_EVENTv2, mysql.UPDATE_ROWS_EVENTv2, mysql.DELETE_ROWS_EVENTv2},
	}
	BinlogDump.AddReplicateDoDb("bifrost_test","*")
	BinlogDump.AddReplicateDoDb("test","*")
	go BinlogDump.StartDumpBinlog(filename, position, 633,reslut,"",0)
	go func() {
		for {
			v := <-reslut
			log.Printf("monitor reslut:%s \r\n", v)
		}
	}()

	for {
		time.Sleep(10 * time.Second)
	}
}