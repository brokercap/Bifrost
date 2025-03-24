//go:build integration
// +build integration

/*
Copyright [2018] [jc3wish]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mongo

import (
	inputDriver "github.com/brokercap/Bifrost/input/driver"
	"testing"
)

func TestMongoInput_GetVersion(t *testing.T) {
	inputInfo := inputDriver.InputInfo{
		ConnectUri:     "mongodb://192.168.137.130:27018",
		GTID:           "",
		BinlogFileName: "mysql-bin.000001",
		BinlogPostion:  0,
		ServerId:       366,
	}
	plugin := NewInputPlugin()
	plugin.SetOption(inputInfo, nil)
	v, err := plugin.GetVersion()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(v)
}

func TestMongoInput_SchemaTableFieldList(t *testing.T) {
	inputInfo := inputDriver.InputInfo{
		ConnectUri:     "mongodb://192.168.137.130:27018",
		GTID:           "",
		BinlogFileName: "mysql-bin.000001",
		BinlogPostion:  0,
		ServerId:       366,
	}
	plugin := NewInputPlugin()
	plugin.SetOption(inputInfo, nil)
	v, err := plugin.GetSchemaTableFieldList("test", "mytb")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(v)
}
