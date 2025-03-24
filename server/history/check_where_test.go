//go:build integration
// +build integration

package history_test

import "testing"

import (
	"github.com/brokercap/Bifrost/server/history"
)

func TestCheckWhere0(t *testing.T) {
	Uri := "root:@tcp(127.0.0.1:3306)/test"
	SchemaName := "bifrost_test"
	TableName := "bristol_performance_test"
	Where1 := "1=1"

	err1 := history.CheckWhere0(Uri, SchemaName, TableName, Where1)

	if err1 == nil {
		t.Log(" Where1 test success ")
	} else {
		t.Fatal(err1)
	}

	Where2 := "asf order "

	err2 := history.CheckWhere0(Uri, SchemaName, TableName, Where2)
	if err2 != nil {
		t.Log(" Where2 test success")
		t.Log("discover err:", err2)
	} else {
		t.Fatal(" Where2 err not be discover ")
	}

}
