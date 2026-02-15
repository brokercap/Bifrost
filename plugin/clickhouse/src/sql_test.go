package src

import (
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"testing"
)

func TestIsNonDDLQuery(t *testing.T) {
	type testCase struct {
		query    string
		expected bool
	}
	cases := []testCase{
		// DDL statements - should return false
		{"ALTER TABLE test ADD COLUMN name VARCHAR(50)", false},
		{"RENAME TABLE old_table TO new_table", false},
		{"DROP TABLE IF EXISTS test", false},
		{"TRUNCATE TABLE test", false},
		{"CREATE TABLE test (id INT)", false},

		// Transaction control - should return true
		{"BEGIN", false},       // BEGIN is handled separately in Query()
		{"COMMIT", false},      // COMMIT is handled separately in Query()
		{"SAVEPOINT sp1", true},
		{"RELEASE SAVEPOINT sp1", true},
		{"ROLLBACK TO sp1", true},
		{"ROLLBACK TO SAVEPOINT sp1", true},

		// DML statements from STATEMENT/MIXED binlog format - should return true
		{"INSERT INTO test VALUES (1, 'foo')", true},
		{"UPDATE test SET name = 'bar' WHERE id = 1", true},
		{"DELETE FROM test WHERE id = 1", true},
		{"REPLACE INTO test VALUES (1, 'foo')", true},

		// Edge cases
		{"", false},
		{"AB", false},
		{"  INSERT INTO test VALUES (1)", true},
		{"  SAVEPOINT sp1  ", true},
		{"insert into test values (1)", true},
		{"update test set name = 'bar'", true},
		{"delete from test where id = 1", true},
		{"savepoint sp1", true},
	}

	for i, c := range cases {
		result := IsNonDDLQuery(c.query)
		if result != c.expected {
			t.Errorf("case %d: IsNonDDLQuery(%q) = %v, want %v", i, c.query, result, c.expected)
		}
	}
}

func TestTranferQuerySql_DML(t *testing.T) {
	// Verify that TranferQuerySql returns empty strings for DML statements.
	// This confirms DML from STATEMENT/MIXED binlog format won't be
	// mistakenly executed as DDL on ClickHouse.
	ckObj := &Conn{
		p: &PluginParam{
			CkSchema: "",
		},
	}

	dmlQueries := []string{
		"INSERT INTO test VALUES (1, 'foo')",
		"UPDATE test SET name = 'bar' WHERE id = 1",
		"DELETE FROM test WHERE id = 1",
		"REPLACE INTO test VALUES (1, 'foo')",
		"SAVEPOINT sp1",
		"RELEASE SAVEPOINT sp1",
		"ROLLBACK TO sp1",
	}

	for i, query := range dmlQueries {
		data := &pluginDriver.PluginDataType{
			Query:      query,
			SchemaName: "test_db",
			TableName:  "test",
		}
		_, _, newSql, newLocalSql, newDisSql, newViewSql := ckObj.TranferQuerySql(data)
		if newSql != "" || newLocalSql != "" || newDisSql != "" || newViewSql != "" {
			t.Errorf("case %d: TranferQuerySql(%q) returned non-empty SQL, expected all empty for DML", i, query)
		}
	}
}

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
