package main

import (
	"log"

	"github.com/jc3wish/Bristol/mysql"
	"time"
	"reflect"
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

	//89

	DBsource = "root:root123@tcp(10.40.6.89:3306)/test"
	filename = "mysql-bin.000078"
	position = 43564768

	DBsource = "root:root123@tcp(10.40.6.89:3306)/test"
	filename = "mysql-bin.000078"
	position = 44125248

	/*
	DBsource = "root:root@tcp(10.40.2.41:3306)/test"
	filename = "mysql-bin.000072"
	position =
	*/


	reslut := make(chan error, 1)
	m := make(map[string]uint8, 0)
	m["bifrost_test"] = 1
	m["mysql"] = 1
	BinlogDump := &mysql.BinlogDump{
		DataSource:    DBsource,
		CallbackFun:   callback,
		ReplicateDoDb: m,
		OnlyEvent:     []mysql.EventType{mysql.QUERY_EVENT, mysql.WRITE_ROWS_EVENTv1, mysql.UPDATE_ROWS_EVENTv1, mysql.DELETE_ROWS_EVENTv1,mysql.WRITE_ROWS_EVENTv2, mysql.UPDATE_ROWS_EVENTv2, mysql.DELETE_ROWS_EVENTv2},
	}
	go BinlogDump.StartDumpBinlog(filename, position, 100,reslut,"",0)
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