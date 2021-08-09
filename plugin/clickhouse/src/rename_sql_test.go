package src

import (
	"strings"
	"testing"
)

func TestReNameSQL_Transfer2CkSQL(t *testing.T) {
	ckObj := &Conn{
		p: &PluginParam{
			CkSchema: "",
		},
	}

	sql := "rename table mytest22 to mytest;"
	Query := ReplaceBr(sql)
	Query = ReplaceTwoReplace(Query)
	Query = strings.Trim(strings.Trim(strings.Trim(Query, " "), ";"), " ")
	c := NewReNameSQL("bifrost_test", Query, ckObj)
	_, _, _, _, destAlterSql := c.Transfer2CkSQL(ckObj)
	t.Log(destAlterSql)
}
