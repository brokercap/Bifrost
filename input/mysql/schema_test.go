package mysql

import (
	inputDriver "github.com/brokercap/Bifrost/input/driver"
	"testing"
)

func TestMysqlInput_GetCurrentPosition(t *testing.T) {
	 o := NewInputPlugin()
	 inputInfo := inputDriver.InputInfo{
		 IsGTID:         false,
		 ConnectUri:     "root:root@tcp(192.168.220.130:3308)/mysql",
		 GTID:           "",
		 BinlogFileName: "",
		 BinlogPostion:  0,
		 ServerId:       0,
		 MaxFileName:    "",
		 MaxPosition:    0,
	 }
	 o.SetOption(inputInfo,nil)
	 p,err := o.GetCurrentPosition()
	 if err != nil {
	 	t.Fatal(err)
	 }
	 t.Log(*p)
}

func TestMysqlInput_GetVersion(t *testing.T) {
	o := NewInputPlugin()
	inputInfo := inputDriver.InputInfo{
		IsGTID:         false,
		ConnectUri:     "root:root@tcp(192.168.220.130:3308)/mysql",
		GTID:           "",
		BinlogFileName: "",
		BinlogPostion:  0,
		ServerId:       0,
		MaxFileName:    "",
		MaxPosition:    0,
	}
	o.SetOption(inputInfo,nil)
	p,err := o.GetVersion()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(p)
}