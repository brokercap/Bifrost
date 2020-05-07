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
