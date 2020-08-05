package src_test

import "testing"

func TestCommitLogAppend(t *testing.T){
	testBefore()
	initDBTable(true)
	initSyncParam()
	param := getParam()
	param["SyncType"] = "insertAll"
	conn.SetParam(param)
	insertdata := event.GetTestInsertData()
	conn.Insert(insertdata)
	conn.Del(event.GetTestDeleteData())
	conn.Update(event.GetTestUpdateData())
	conn.Insert(event.GetTestInsertData())
	conn.Del(event.GetTestDeleteData())

	_,err2 := conn.Commit()
	if err2 != nil{
		t.Fatal(err2)
	}
}


func TestCommitLogUpdate(t *testing.T){
	testBefore()
	initDBTable(true)
	initSyncParam()
	param := getParam()
	param["SyncType"] = "LogUpdate"
	conn.SetParam(param)
	insertdata := event.GetTestInsertData()
	conn.Insert(insertdata)
	conn.Del(event.GetTestDeleteData())
	_,err2 := conn.Commit()
	if err2 != nil{
		t.Fatal(err2)
	}
}

func TestCommitLogAppendReplacingMergeTree(t *testing.T){
	engine = "ReplacingMergeTree(id)"
	testBefore()
	initDBTable(true)
	initSyncParam()
	param := getParam()
	param["SyncType"] = "insertAll"
	conn.SetParam(param)
	for j:=0;j<10;j++{
		insertdata := event.GetTestInsertData()
		conn.Insert(insertdata)
		for i:=0;i<100000;i++{
			conn.Update(event.GetTestUpdateData())
		}
		conn.Del(event.GetTestDeleteData())
	}
	_,err2 := conn.Commit()
	if err2 != nil{
		t.Fatal(err2)
	}
}

