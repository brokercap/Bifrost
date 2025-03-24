package main

/*
import (
	"github.com/brokercap/Bifrost/plugin/driver"
	_ "github.com/brokercap/Bifrost/plugin/redis/src"
	"log"
	_ "net/http/pprof"
	"net/http"
	"time"
	"github.com/brokercap/Bifrost/plugin/redis/src"
)

func main()  {
	f1 := src.MyConn{}
	f := f1.Open("127.0.0.1:6379")
	redisKeyPluginPamram := make(map[string]interface{},0)
	redisKeyPluginPamram["KeyConfig"] = "{$SchemaName}-{$TableName}"
	redisKeyPluginPamram["DataType"] = "json"
	redisKeyPluginPamram["Type"] = "set"

	go func() {
		http.ListenAndServe("0.0.0.0:10000", nil)
	}()

	var p interface{}
	p = redisKeyPluginPamram
	var err error
	for i:=0;i<100000;i++ {
		m := make(map[string]interface{},0)
		m["ssdfsdfsdfsdfsdf1"] = "sdfsdfsdffffffffffffff"
		m["ssdfsdfsdfsdfsdf2"] = "sdfsdfsdffffffffffffff"
		m["ssdfsdfsdfsdfsdf3"] = "sdfsdfsdffffffffffffff"
		m["ssdfsdfsdfsdfsdf4"] = "sdfsdfsdffffffffffffff"
		m["ssdfsdfsdfsdfsdf5"] = "sdfsdfsdffffffffffffff"
		m["ssdfsdfsdfsdfsdf6"] = "sdfsdfsdffffffffffffff"
		m["ssdfsdfsdfsdfsdf7"] = "sdfsdfsdffffffffffffff"
		m["ssdfsdfsdfsdfsdf8"] = "sdfsdfsdffffffffffffff"
		m["ssdfsdfsdfsdfsdf9"] = "sdfsdfsdffffffffffffff"
		m["ssdfsdfsdfsdfsdf10"] = "sdfsdfsdffffffffffffff"
		m["ssdfsdfsdfsdfsdf11"] = "sdfsdfsdffffffffffffff"
		m["ssdfsdfsdfsdfsdf12"] = "sdfsdfsdffffffffffffff"
		m["ssdfsdfsdfsdfsdf12"] = "sdfsdfsdffffffffffffff"
		m["ssdfsdfsdfsdfsdf14"] = "sdfsdfsdffffffffffffff"

		rows := make([]map[string]interface{},1)
		data := driver.PluginDataType{
			Timestamp:uint32(time.Now().Unix()),
			EventType:"insert",
			Rows:rows,
			Query:"",
			SchemaName:"test",
			TableName:"testtable",
			BinlogFileNum:70,
			BinlogPosition:12341451,
		}

		p,err = f.SetParam(p)
		if err != nil{
			log.Println(err)
			continue
		}
		f.Insert(&data)
	}
	ch := make(chan int,1)

	for{
		<- ch
	}

}
*/
