//go:build integration
// +build integration

package src

import (
	"testing"

	"github.com/brokercap/Bifrost/sdk/pluginTestData"
)

func TestAppendSyncAndChekcData(t *testing.T) {
	beforeTest()
	initDBTable(true)
	conn := getPluginConn("LogAppend")
	e := pluginTestData.NewEvent()
	insertdata := e.GetTestInsertData()
	conn.Insert(insertdata, false)
	updateData := e.GetTestUpdateData()
	conn.Update(updateData, false)
	deleteData := e.GetTestDeleteData()
	conn.Del(deleteData, false)
	_, _, err2 := conn.TimeOutCommit()
	if err2 != nil {
		t.Fatal(err2)
	}

	n, err := getTableCount()
	if err != nil {
		t.Fatal(err)
	}

	if n != 3 {
		t.Fatal("append result count != 3")
	}
	t.Log("success")
}
