package src

import (
	"strings"
	"testing"
)

func TestReNameSQL_Transfer2CkSQL(t *testing.T) {
	ckObj := &Conn{
		p: &PluginParam{
			CkSchema: "",
			CkEngine: 1,
			ModifDDLType: &DDLSupportType{
				TableRename: true,
			},
		},
	}

	sql := "rename table mytest22 to mytest;"
	Query := ReplaceBr(sql)
	Query = ReplaceTwoReplace(Query)
	Query = strings.Trim(strings.Trim(strings.Trim(Query, " "), ";"), " ")
	c := NewReNameSQL("bifrost_test", Query, ckObj)
	_, _, destAlterSql, _, _ := c.Transfer2CkSQL(ckObj)
	var mustBeDestAlterSQl = "RENAME TABLE bifrost_test.mytest22 TO bifrost_test.mytest"
	if destAlterSql != mustBeDestAlterSQl {
		t.Fatalf("err destAlterSql:%s", destAlterSql)
	}
	t.Log("test over!")
}

func TestClusterEngine_ReNameSQL_Transfer2CkSQL(t *testing.T) {
	ckObj := &Conn{
		p: &PluginParam{
			CkSchema: "",
			CkEngine: 2,
		},
	}

	sql := "rename table mytest22 to mytest;"
	Query := ReplaceBr(sql)
	Query = ReplaceTwoReplace(Query)
	Query = strings.Trim(strings.Trim(strings.Trim(Query, " "), ";"), " ")
	c := NewReNameSQL("bifrost_test", Query, ckObj)
	SchemaName, TableName, destAlterSql, destAlterViewSql, destAlterDisSql := c.Transfer2CkSQL(ckObj)

	if destAlterDisSql == "" && destAlterViewSql == "" {
		t.Log("test over!")
	} else {
		t.Errorf("SchemaName:%s", SchemaName)
		t.Errorf("TableName:%s", TableName)
		t.Errorf("destAlterSql:%s", destAlterSql)
		t.Errorf("destAlterViewSql:%s", destAlterViewSql)
		t.Errorf("destAlterDisSql:%s", destAlterDisSql)
		t.Errorf("not supported cluster rename!")
	}
}
