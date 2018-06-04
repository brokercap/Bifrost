package main

import (
	"log"

	"github.com/jc3wish/Bifrost/Bristol/mysql"
	"time"
)

func callback(data *mysql.EventReslut) {
	log.Println(data)
}

func main() {
	filename := "mysql-bin.000022"
	var position uint32 = 13333
	reslut := make(chan error, 1)
	m := make(map[string]uint8, 0)
	m["testdbcreate"] = 1
	BinlogDump := &mysql.BinlogDump{
		DataSource:    "root:root@tcp(127.0.0.1:3306)/test",
		CallbackFun:   callback,
		ReplicateDoDb: m,
		OnlyEvent:     []mysql.EventType{mysql.QUERY_EVENT, mysql.WRITE_ROWS_EVENTv1, mysql.UPDATE_ROWS_EVENTv1, mysql.DELETE_ROWS_EVENTv1},
	}
	go BinlogDump.StartDumpBinlog(filename, position, 100,reslut,"",0)
	go func() {
		v := <-reslut
		log.Printf("monitor reslut:%s \r\n", v)
	}()
	for {
		time.Sleep(10 * time.Second)
	}
}
