package mongo

import (
	inputDriver "github.com/brokercap/Bifrost/input/driver"
	outputDriver "github.com/brokercap/Bifrost/plugin/driver"
	"log"
	"testing"
	"time"
)

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
			if p == nil {
				continue
			}
			t.Log("position:", *p)
			break
		}
	}
}

func callback(data *outputDriver.PluginDataType) {
	log.Println("callback data:", *data)
}

func TestMongoInput_Start(t *testing.T) {
	inputInfo := inputDriver.InputInfo{
		ConnectUri:     "mongodb://192.168.137.130:27017",
		GTID:           "",
		BinlogFileName: "mysql-bin.000001",
		BinlogPostion:  0,
		ServerId:       366,
	}
	ch := make(chan *inputDriver.PluginStatus, 2)
	plugin := NewInputPlugin()
	plugin.SetEventID(0)
	plugin.SetOption(inputInfo, nil)
	plugin.SetCallback(callback)
	go plugin.Start(ch)
	go monitorDump(ch, plugin, t)
	time.Sleep(1000 * time.Second)
}
