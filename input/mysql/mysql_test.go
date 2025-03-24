//go:build integration
// +build integration

package mysql

import (
	inputDriver "github.com/brokercap/Bifrost/input/driver"
	outputDriver "github.com/brokercap/Bifrost/plugin/driver"
	"log"
	"testing"
	"time"
)

var mysql_uri = "root:root@tcp(bifrost_mysql_test:3306)/mysql?charset=utf8"

func TestMysqlInput_GetUriExample(t *testing.T) {

}

func monitorDump(reslut chan *inputDriver.PluginStatus, plugin inputDriver.Driver, t *testing.T) (r bool) {
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for {
		select {
		case v := <-reslut:
			timer.Reset(3 * time.Second)
			t.Log("status:", v)
		case <-timer.C:
			timer.Reset(3 * time.Second)
			p, _ := plugin.GetCurrentPosition()
			t.Log("position:", *p)
			break
		}
	}
}

func callback(data *outputDriver.PluginDataType) {
	log.Println("callback data:", *data)
}

func TestMysqlInput_Start_Integration(t *testing.T) {
	inputInfo := inputDriver.InputInfo{
		ConnectUri:     mysql_uri,
		GTID:           "",
		BinlogFileName: "mysql-bin.000021",
		BinlogPostion:  156,
		ServerId:       366,
	}
	ch := make(chan *inputDriver.PluginStatus, 2)
	plugin := NewInputPlugin()
	plugin.SetEventID(0)
	plugin.SetOption(inputInfo, nil)
	plugin.SetCallback(callback)
	go plugin.Start(ch)
	go monitorDump(ch, plugin, t)
	time.Sleep(50 * time.Second)
}
