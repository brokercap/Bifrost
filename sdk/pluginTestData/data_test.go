package pluginTestData

import (
	"testing"
	"reflect"
)

func TestGetTestData(t *testing.T){
	e := NewEvent()

	t.Log("GetTestInsertData:", e.GetTestInsertData())

	t.Log("")

	t.Log("GetTestUpdateData:", e.GetTestUpdateData())

	t.Log("")

	t.Log("GetTestDeleteData:", e.GetTestDeleteData())

	t.Log("")

	t.Log("GetTestQueryData:", e.GetTestQueryData())
}

func TestGetTestDataCheck(t *testing.T){
	e := NewEvent()
	data := e.GetTestInsertData()
	m := data.Rows[0]
	for _,columnType := range e.ColumnList{
		if _,ok := m[columnType.ColumnName];!ok{
			t.Error(columnType.ColumnName," not esxit")
			continue
		}
		t.Log(columnType.ColumnName,"==",m[columnType.ColumnName],"(",reflect.TypeOf(m[columnType.ColumnName]),")")
	}

}
