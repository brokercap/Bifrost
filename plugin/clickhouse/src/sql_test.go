package src

import "testing"

func TestConn_getAutoTableSqlSchemaAndTable(t *testing.T) {
	type caseStruct struct {
		dbAndTable        string
		DefaultSchemaName string
		ResultSchemaName  string
		ResultTableName   string
		ConnCkSchema      string
	}
	var caseArr = []caseStruct{
		{
			dbAndTable:        "bifrost_test.binlog_field_test",
			DefaultSchemaName: "test",
			ResultSchemaName:  "bifrost_test",
			ResultTableName:   "binlog_field_test",
		},
		{
			dbAndTable:        "binlog_field_test",
			DefaultSchemaName: "test",
			ResultSchemaName:  "test",
			ResultTableName:   "binlog_field_test",
		},
		{
			dbAndTable:        "bifrost_test.binlog_field_test",
			DefaultSchemaName: "test",
			ResultSchemaName:  "xxtest",
			ResultTableName:   "binlog_field_test",
			ConnCkSchema:      "xxtest",
		},
		{
			dbAndTable:        "`bifrost_test`.`binlog_field_test`",
			DefaultSchemaName: "test",
			ResultSchemaName:  "bifrost_test",
			ResultTableName:   "binlog_field_test",
		},
		{
			dbAndTable:        "`binlog_field_test`",
			DefaultSchemaName: "test",
			ResultSchemaName:  "test",
			ResultTableName:   "binlog_field_test",
		},
		{
			dbAndTable:        "`bifrost_test`.`binlog_field_test`",
			DefaultSchemaName: "test",
			ResultSchemaName:  "xxtest",
			ResultTableName:   "binlog_field_test",
			ConnCkSchema:      "xxtest",
		},
	}

	var f = func(i int, caseInfo caseStruct) {
		ckObj := &Conn{
			p: &PluginParam{
				CkSchema: caseInfo.ConnCkSchema,
			},
		}
		ResultSchemaName, ResultTableName := ckObj.getAutoTableSqlSchemaAndTable(caseInfo.dbAndTable, caseInfo.DefaultSchemaName)
		if ResultSchemaName != caseInfo.ResultSchemaName {
			t.Errorf("i:%d ResultSchemaName: %s != %s (dest) ", i, ResultSchemaName, caseInfo.ResultSchemaName)
		}
		if ResultTableName != caseInfo.ResultTableName {
			t.Errorf("i:%d ResultTableName: %s != %s (dest) ", i, ResultTableName, caseInfo.ResultTableName)
		}
	}

	for i, caseInfo := range caseArr {
		f(i, caseInfo)
	}
	t.Log("test over!")
}
