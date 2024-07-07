package src

import (
	"testing"
)

func TestNewTableData(t *testing.T) {
	c := NewTableData()
	if c.CommitData[0] == nil {
		t.Log("test frist 0 index is nil")
	}
	c.CommitData = c.CommitData[1:]
	t.Log("success")
}

func TestConn_InitVersion0(t *testing.T) {
	obj := &Conn{}
	str := "19.13.3.26"
	str2 := "19.12.31.26"
	v1 := obj.InitVersion0(str)
	v2 := obj.InitVersion0(str2)
	if v1 > v2 {
		t.Log("str:", str, " ==> ", v1)
		t.Log("str2:", str2, " ==> ", v2)
	} else {
		t.Error("str:", str, " ==> ", v1)
		t.Error("str2:", str2, " ==> ", v2)
		t.Fatal("")
	}

	str3 := "19.13.3"
	v3 := obj.InitVersion0(str3)
	if v3 == 1913030000 {
		t.Log("str3:", str3, " ==> ", v3)
		t.Log("success")
	} else {
		t.Fatal("str3:", str3, " ==> ", v3)
	}
}
