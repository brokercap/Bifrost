package src

import (
	"testing"
	"time"
)

var dbname string = "testDbName"
var schemaName string = "bifrost_test"
var tableName string = "binlog_field_test"

func TestAddCount(t *testing.T) {

	AddCount(dbname, schemaName, tableName, INSERT, 10, true)
	AddCount(dbname, schemaName, tableName, INSERT, 1, false)

	time.Sleep(time.Duration(10) * time.Second)
	t.Log("test success")
}
