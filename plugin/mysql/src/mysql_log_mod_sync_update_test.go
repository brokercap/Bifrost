//go:build integration
// +build integration

package src

import (
	"github.com/brokercap/Bifrost/sdk/pluginTestData"
	"testing"
)

func TestUpdateSyncAndChekcData(t *testing.T) {
	beforeTest()
	initDBTable(true)
	conn := getPluginConn("LogUpdate")
	e := pluginTestData.NewEvent()
	insertdata := e.GetTestInsertData()
	conn.Insert(insertdata, false)
	conn.Insert(e.GetTestInsertData(), false)
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

	if n != 2 {
		t.Fatal("append result count != 1")
	}
	t.Log("success")
}
