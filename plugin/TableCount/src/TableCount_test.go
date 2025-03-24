//go:build integration
// +build integration

package src

import "testing"

import (
	"github.com/brokercap/Bifrost/sdk/pluginTestData"
)

var url = "TableCount"

var event *pluginTestData.Event
var SchemaName = "bifrost_test"
var TableName = "binlog_field_test"

func testBefore() {
	event = pluginTestData.NewEvent()
	event.SetSchema(SchemaName)
	event.SetTable(TableName)
}

func getParam() map[string]interface{} {
	p := make(map[string]interface{}, 0)
	p["DbName"] = dbname
	return p
}

func TestChechUri(t *testing.T) {
	myConn := NewConn()
	myConn.SetOption(nil, nil)
	if err := myConn.CheckUri(); err != nil {
		t.Fatal("TestChechUri err:", err)
	} else {
		t.Log("TestChechUri success")
	}
}

func TestSetParam(t *testing.T) {
	myConn := NewConn()
	myConn.SetOption(nil, nil)
	myConn.Open()
	_, err := myConn.SetParam(getParam())
	if err != nil {
		t.Fatal(err)
	}
	t.Log("success")
}

func TestInsert(t *testing.T) {
	testBefore()
	myConn := NewConn()
	myConn.SetOption(nil, nil)
	myConn.Open()
	_, _, err := myConn.Insert(event.GetTestInsertData(), false)
	if err != nil {
		t.Fatal(err)
	}
}

func TestUpate(t *testing.T) {
	testBefore()
	myConn := NewConn()
	myConn.SetOption(nil, nil)
	myConn.Open()
	_, _, err := myConn.Update(event.GetTestUpdateData(), false)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDelete(t *testing.T) {
	testBefore()
	myConn := NewConn()
	myConn.SetOption(nil, nil)
	myConn.Open()
	_, _, err := myConn.Del(event.GetTestDeleteData(), false)
	if err != nil {
		t.Fatal(err)
	}
}

func TestQuery(t *testing.T) {
	testBefore()
	myConn := NewConn()
	myConn.SetOption(nil, nil)
	myConn.Open()
	_, _, err := myConn.Query(event.GetTestQueryData(), false)
	if err != nil {
		t.Fatal(err)
	}
}

func TestCommit(t *testing.T) {
	testBefore()
	myConn := NewConn()
	myConn.SetOption(nil, nil)
	myConn.Open()
	_, _, err := myConn.Commit(event.GetTestCommitData(), false)
	if err != nil {
		t.Fatal(err)
	}
}

// 模拟正式环境刷数据
func TestSyncLikeProduct(t *testing.T) {
	p := pluginTestData.NewPlugin("TableCount", url)
	err0 := p.SetParam(getParam())
	p.SetEventType(pluginTestData.INSERT)
	if err0 != nil {
		t.Fatal(err0)
	}

	var n uint = 10000
	err := p.DoTestStart(n)

	if err != nil {
		t.Fatal(err)
	} else {
		t.Log("test success")
	}
}
