package main

import (
	"log"

	"github.com/brokercap/Bifrost/Bristol/mysql"
	"time"
	"fmt"
	"flag"
)

func callback(data *mysql.EventReslut) {
	if data.TableName == "review"{
		log.Println(data)
	}
	return
	log.Println(data)
	//e := encoding.Decoder{}
	if data.Query == ""{
		for k,v := range data.Rows[len(data.Rows)-1]{
			log.Println(k,"==",fmt.Sprint(v))
			/*
			switch v.(type) {
			case string:
				a,err := e.String(v.(string))
				if err != nil{
					log.Println(k, "Decoder err", err, "v:",v)
				}else{
					log.Println(k,"==",a)
				}

				break
			default:
				log.Println(k,"==",v)
				break
			}
			*/

		}
	}
}

func main() {
	filename := "mysql-bin.000071"

	var position uint32 = 203785789
	var DBsource = ""

	//41
	DBsource = "root:root@tcp(10.40.2.41:3306)/test"
	filename = "mysql-bin.000072"
	position = 9487
	position = 16750

	//89

	DBsource = "root:root123@tcp(10.40.6.89:3306)/test"
	filename = "mysql-bin.000078"
	position = 43564768

	DBsource = "root:root123@tcp(10.40.6.89:3306)/test"
	filename = "mysql-bin.000078"
	position = 44125248

	DBsource = "root:root@tcp(10.40.2.41:3306)/test"
	filename = "mysql-bin.000072"
	position = 17902

	DBsource = "root:123456@tcp(45.40.207.2:3311)/mysql"
	filename = "mysql-bin.000004"
	position = 107

	binlogFileName := flag.String("binlogFileName", "", "")
	binlogPostion := flag.Int("binlogPostion", 4, "")
	flag.Parse()

	if *binlogPostion != 4{
		position = uint32(*binlogPostion)
	}

	if *binlogFileName != ""{
		filename = *binlogFileName
	}

	reslut := make(chan error, 1)

	BinlogDump := mysql.NewBinlogDump(
		DBsource,
		callback,
		[]mysql.EventType{mysql.QUERY_EVENT, mysql.WRITE_ROWS_EVENTv1, mysql.UPDATE_ROWS_EVENTv1, mysql.DELETE_ROWS_EVENTv1,mysql.WRITE_ROWS_EVENTv2, mysql.UPDATE_ROWS_EVENTv2, mysql.DELETE_ROWS_EVENTv2},
		nil,nil)
	//BinlogDump.AddReplicateDoDb("bifrost_test","*")
	go BinlogDump.StartDumpBinlog(filename, position, 5869,reslut,"",0)
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
