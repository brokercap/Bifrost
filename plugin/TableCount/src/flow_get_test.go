package src

import (
	"testing"
	"time"
)

func AddTestData() {
	AddCount(dbname, schemaName, tableName, INSERT, 10, true)

	AddCount(dbname, schemaName, tableName, INSERT, 1, false)

	time.Sleep(time.Duration(6) * time.Second)

	AddCount(dbname, schemaName, tableName, UPDATE, 1, true)

	AddCount(dbname, schemaName, tableName+"_2", UPDATE, 16, true)
	AddCount(dbname, schemaName, tableName+"_3", UPDATE, 2, true)

	AddCount(dbname, schemaName, tableName, INSERT, 5, false)

	time.Sleep(time.Duration(5) * time.Second)

	AddCount(dbname, schemaName, tableName, INSERT, 10, false)
	time.Sleep(time.Duration(5) * time.Second)
}

func TestGetFlow(t *testing.T) {

	AddTestData()

	data, err := GetFlow("TenMinute", dbname, schemaName, tableName)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(data)
}

func TestGetFlowBySchema(t *testing.T) {

	AddTestData()

	data, err := GetFlowBySchema("TenMinute", dbname, schemaName)
	if err != nil {
		t.Fatal(err)
	}

	for _, Count := range data {
		if Count.Time == 0 {
			continue
		}
		t.Log("InsertCount:", Count.InsertCount)
		t.Log("UpdateCount:", Count.UpdateCount)
		t.Log("DeleteCount:", Count.DeleteCount)
		t.Log("InsertRows:", Count.InsertRows)
		t.Log("UpdateRows:", Count.UpdateRows)
		t.Log("DeleteRows:", Count.DeleteRows)
		t.Log("DDLCount:", Count.DDLCount)
		t.Log("")
	}

	t.Log(data)
}

func TestGetFlowByDbName(t *testing.T) {

	AddTestData()

	data, err := GetFlowByDbName("TenMinute", dbname)
	if err != nil {
		t.Fatal(err)
	}

	for _, Count := range data {
		if Count.Time == 0 {
			continue
		}
		t.Log("InsertCount:", Count.InsertCount)
		t.Log("UpdateCount:", Count.UpdateCount)
		t.Log("DeleteCount:", Count.DeleteCount)
		t.Log("InsertRows:", Count.InsertRows)
		t.Log("UpdateRows:", Count.UpdateRows)
		t.Log("DeleteRows:", Count.DeleteRows)
		t.Log("DDLCount:", Count.DDLCount)
		t.Log("")
	}

	t.Log(data)
}
