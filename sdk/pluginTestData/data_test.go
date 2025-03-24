package pluginTestData

import (
	"strings"
	"testing"
	"unsafe"
)

func TestGetTestData(t *testing.T) {
	e := NewEvent()

	data := e.GetTestInsertData()

	t.Log("GetTestInsertData:", data)
	t.Log("id:", data.Rows[0]["id"])

	t.Log("")

	data = e.GetTestInsertData()

	t.Log("GetTestInsertData:", data)
	t.Log("id:", data.Rows[0]["id"])

	t.Log("")

	data = e.GetTestInsertData()

	t.Log("GetTestInsertData:", data)
	t.Log("id:", data.Rows[0]["id"])

	t.Log("")

	data = e.GetTestInsertData()

	t.Log("GetTestInsertData:", data)
	t.Log("id:", data.Rows[0]["id"])

	t.Log("")

	data = e.GetTestDeleteData()
	t.Log("GetTestDeleteData:", data)

	t.Log("id:", data.Rows[0]["id"])

	t.Log("")

	data = e.GetTestUpdateData()

	t.Log("GetTestUpdateData:", data)
	t.Log("id:", data.Rows[1]["id"])

	t.Log("")

	t.Log("GetTestQueryData:", e.GetTestQueryData())

	data = e.GetTestInsertData()

	t.Log("GetTestInsertData:", data)
	t.Log("id:", data.Rows[0]["id"])

	t.Log("")

	data = e.GetTestDeleteData()
	t.Log("GetTestDeleteData:", data)

	t.Log("id:", data.Rows[0]["id"])

	data = e.GetTestDeleteData()
	t.Log("GetTestDeleteData:", data)

	t.Log("id:", data.Rows[0]["id"])

	data = e.GetTestDeleteData()
	t.Log("GetTestDeleteData:", data)

	t.Log("id:", data.Rows[0]["id"])

	data = e.GetTestDeleteData()
	t.Log("GetTestDeleteData:", data)

	t.Log("id:", data.Rows[0]["id"])

	data = e.GetTestDeleteData()
	t.Log("GetTestDeleteData:", data)

	t.Log("id:", data.Rows[0]["id"])

	t.Log("")
}

/*
这个单测需要修改，暂时注释
func TestGetTestDataCheck(t *testing.T) {
	e := NewEvent()
	data := e.GetTestInsertData()
	m := data.Rows[0]
	c, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	checkResult, err := e.CheckData(m, string(c))
	if err != nil {
		t.Fatal(err)
	}

	if err != nil {
		t.Fatal(err)
	}

	for _, v := range checkResult["ok"] {
		t.Log(v)
	}

	for _, v := range checkResult["error"] {
		t.Error(v)
	}

	t.Log("test over")
}

*/

// 测试获取null值数据
func TestGetTestNullData(t *testing.T) {
	e := NewEvent()
	e.SetIsNull(true)

	data := e.GetTestInsertData()

	for k, v := range data.Rows[0] {
		if k == "id" {
			t.Log(k, " == ", v)
		} else {
			if strings.Contains(k, "_null") && v != nil {
				t.Error(k, " : ", v, " != nil")
			} else {
				t.Log(k, " == ", v)
			}
		}
	}

	t.Log("test over")
}

func TestSizeOfData(t *testing.T) {
	e := NewEvent()
	e.SetIsNull(true)

	data := e.GetTestInsertData()

	EventSize := unsafe.Pointer(unsafe.Sizeof(data.Rows))

	t.Log("data:", *data)
	t.Log("EventSize:", EventSize)
	t.Log("test over")
}
