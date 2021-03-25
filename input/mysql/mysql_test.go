package mysql_test

import (
	inputDriver "github.com/brokercap/Bifrost/input/driver"
	input "github.com/brokercap/Bifrost/input/mysql"
	outputDriver "github.com/brokercap/Bifrost/plugin/driver"
	"log"
	"testing"
	"time"
)

func TestMysqlInput_GetUriExample(t *testing.T) {

}

func monitorDump(reslut chan *inputDriver.PluginStatus,plugin inputDriver.Driver,t *testing.T) (r bool) {
	timer := time.NewTimer( 3 * time.Second)
	defer timer.Stop()
	for {
		select {
		case v := <-reslut:
			timer.Reset(3 * time.Second)
			t.Log("status:",v)
		case <- timer.C:
			timer.Reset(3 * time.Second)
			p,_ := plugin.GetCurrentPosition()
			t.Log("position:",*p)
			break
		}
	}
}

func callback(data *outputDriver.PluginDataType) {
	log.Println("callback data:",*data)
}

func TestMysqlInput_Start(t *testing.T) {
	inputInfo := inputDriver.InputInfo{
		ConnectUri: "root:root@tcp(192.168.220.130:3308)/mytest",
		GTID:"",
		BinlogFileName: "mysql-bin.000021",
		BinlogPostion: 156,
		ServerId: 366,
	}
	ch := make(chan *inputDriver.PluginStatus,2)
	plugin := input.NewInputPlugin()
	plugin.SetEventID(0)
	plugin.SetOption(inputInfo,nil)
	plugin.SetCallback(callback)
	go plugin.Start(ch)
	go monitorDump(ch,plugin,t)
	time.Sleep(50 * time.Second)
}
