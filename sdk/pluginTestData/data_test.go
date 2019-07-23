package pluginTestData

import (
	"testing"
	"reflect"
)

func TestGetTestData(t *testing.T){
	e := NewEvent()

	data := e.GetTestInsertData()

	t.Log("GetTestInsertData:",data )
	t.Log("id:",data.Rows[0]["id"])

	t.Log("")

	data = e.GetTestDeleteData()
	t.Log("GetTestDeleteData:", data)

	t.Log("id:",data.Rows[0]["id"])

	t.Log("")

	data = e.GetTestUpdateData()

	t.Log("GetTestUpdateData:", data)
	t.Log("id:",data.Rows[1]["id"])



	t.Log("")

	t.Log("GetTestQueryData:", e.GetTestQueryData())


	data = e.GetTestInsertData()

	t.Log("GetTestInsertData:",data )
	t.Log("id:",data.Rows[0]["id"])

	t.Log("")

	data = e.GetTestDeleteData()
	t.Log("GetTestDeleteData:", data)

	t.Log("id:",data.Rows[0]["id"])

	t.Log("")
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
