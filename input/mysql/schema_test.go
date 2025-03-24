//go:build integration
// +build integration

package mysql

import (
	inputDriver "github.com/brokercap/Bifrost/input/driver"
	"testing"
)

func TestMysqlInput_GetCurrentPosition_Integration(t *testing.T) {
	o := NewInputPlugin()
	inputInfo := inputDriver.InputInfo{
		IsGTID:         false,
		ConnectUri:     mysql_uri,
		GTID:           "",
		BinlogFileName: "",
		BinlogPostion:  0,
		ServerId:       0,
		MaxFileName:    "",
		MaxPosition:    0,
	}
	o.SetOption(inputInfo, nil)
	p, err := o.GetCurrentPosition()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(*p)
}

func TestMysqlInput_GetVersion_Integration(t *testing.T) {
	o := NewInputPlugin()
	inputInfo := inputDriver.InputInfo{
		IsGTID:         false,
		ConnectUri:     mysql_uri,
		GTID:           "",
		BinlogFileName: "",
		BinlogPostion:  0,
		ServerId:       0,
		MaxFileName:    "",
		MaxPosition:    0,
	}
	o.SetOption(inputInfo, nil)
	p, err := o.GetVersion()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(p)
}
