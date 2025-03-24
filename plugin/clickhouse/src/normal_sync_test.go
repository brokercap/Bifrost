//go:build integration
// +build integration

package src

import "testing"

func TestCommitLogAppend(t *testing.T) {
	testBefore()
	initDBTable(true)
	initSyncParam()
	param := getParam()
	param["SyncType"] = "insertAll"
	conn.SetParam(param)
	insertdata := event.GetTestInsertData()
	conn.Insert(insertdata, false)
	conn.Del(event.GetTestDeleteData(), false)
	conn.Update(event.GetTestUpdateData(), false)
	conn.Insert(event.GetTestInsertData(), false)
	conn.Del(event.GetTestDeleteData(), false)

	_, _, err2 := conn.TimeOutCommit()
	if err2 != nil {
		t.Fatal(err2)
	}
	t.Log("success")
}

func TestCommitLogUpdate(t *testing.T) {
	testBefore()
	initDBTable(true)
	initSyncParam()
	param := getParam()
	param["SyncType"] = "LogUpdate"
	conn.SetParam(param)
	insertdata := event.GetTestInsertData()
	conn.Insert(insertdata, false)
	conn.Del(event.GetTestDeleteData(), false)
	_, _, err2 := conn.TimeOutCommit()
	if err2 != nil {
		t.Fatal(err2)
	}
	t.Log("success")
}

func TestCommitLogAppendReplacingMergeTree(t *testing.T) {
	engine = "ReplacingMergeTree(id)"
	testBefore()
	initDBTable(true)
	initSyncParam()
	param := getParam()
	param["SyncType"] = "insertAll"
	conn.SetParam(param)
	for j := 0; j < 10; j++ {
		insertdata := event.GetTestInsertData()
		conn.Insert(insertdata, false)
		for i := 0; i < 1000; i++ {
			conn.Update(event.GetTestUpdateData(), false)
		}
		conn.Del(event.GetTestDeleteData(), false)
	}
	_, _, err2 := conn.TimeOutCommit()
	if err2 != nil {
		t.Fatal(err2)
	}
	t.Log("success")
}
