//go:build integration
// +build integration

package src

import (
	"github.com/brokercap/Bifrost/sdk/pluginTestData"
	"log"
	"testing"
)

var url string = "http://127.0.0.1:3332/bifrost_http_api_test"
var event *pluginTestData.Event

var SchemaName = "bifrost_test"
var TableName = "binlog_field_test"

func init() {
	event = pluginTestData.NewEvent()
	event.SetSchema(SchemaName)
	event.SetTable(TableName)
}
func TestChechUri(t *testing.T) {
	myConn := NewConn()
	myConn.SetOption(&url, nil)
	if err := myConn.CheckUri(); err != nil {
		log.Println("TestChechUri err:", err)
	} else {
		log.Println("TestChechUri success")
	}
}

func TestSetParam(t *testing.T) {
	myConn := NewConn()
	myConn.SetOption(&url, nil)
	myConn.SetParam(nil)
}

func TestInsert(t *testing.T) {
	conn := NewConn()
	conn.SetOption(&url, nil)
	conn.Insert(event.GetTestInsertData(), false)
}

func TestUpate(t *testing.T) {
	conn := NewConn()
	conn.SetOption(&url, nil)
	conn.Update(event.GetTestUpdateData(), false)
}

func TestDelete(t *testing.T) {
	conn := NewConn()
	conn.SetOption(&url, nil)
	conn.Del(event.GetTestDeleteData(), false)
}

func TestQuery(t *testing.T) {
	conn := NewConn()
	conn.SetOption(&url, nil)
	conn.Query(event.GetTestQueryData(), false)
}

func TestCommit(t *testing.T) {
	conn := NewConn()
	conn.SetOption(&url, nil)
	conn.Commit(event.GetTestCommitData(), false)
}
