package src

import (
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
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

func TestQuery_NonDDLStatements(t *testing.T) {
	// Verify that non-DDL QUERY_EVENTs (DML and transaction control) don't
	// trigger AutoCommit and don't lose pending data. This tests the fix for
	// issue #285: MySQL trigger data loss when syncing to ClickHouse.
	conn := &Conn{
		p: &PluginParam{
			AutoCreateTable: true,
			BatchSize:       500,
			Data:            NewTableData(),
		},
	}

	// Simulate pending row data in the buffer
	conn.p.Data.Data = append(conn.p.Data.Data, &pluginDriver.PluginDataType{
		EventType:  "insert",
		SchemaName: "test_db",
		TableName:  "test_table",
		Rows:       []map[string]interface{}{{"id": 1, "name": "foo"}},
	})

	nonDDLQueries := []string{
		"INSERT INTO test VALUES (1, 'foo')",
		"UPDATE test SET name = 'bar'",
		"DELETE FROM test WHERE id = 1",
		"REPLACE INTO test VALUES (1, 'foo')",
		"SAVEPOINT sp1",
		"RELEASE SAVEPOINT sp1",
		"ROLLBACK TO sp1",
	}

	for _, query := range nonDDLQueries {
		// Re-add pending data for each iteration (to verify it's preserved)
		if len(conn.p.Data.Data) == 0 {
			conn.p.Data.Data = append(conn.p.Data.Data, &pluginDriver.PluginDataType{
				EventType:  "insert",
				SchemaName: "test_db",
				TableName:  "test_table",
				Rows:       []map[string]interface{}{{"id": 1, "name": "foo"}},
			})
		}

		data := &pluginDriver.PluginDataType{
			Query:      query,
			SchemaName: "test_db",
			TableName:  "test_table",
		}

		lastSuccess, errData, err := conn.Query(data, false)
		if lastSuccess != nil {
			t.Errorf("Query(%q): expected nil lastSuccess, got %v", query, lastSuccess)
		}
		if errData != nil {
			t.Errorf("Query(%q): expected nil errData, got %v", query, errData)
		}
		if err != nil {
			t.Errorf("Query(%q): expected nil err, got %v", query, err)
		}

		// Verify pending data was NOT flushed by AutoCommit
		if len(conn.p.Data.Data) == 0 {
			t.Errorf("Query(%q): pending data was flushed (AutoCommit triggered), expected data to be preserved", query)
		}
	}
}

func TestQuery_TransactionControlSkipped(t *testing.T) {
	// Verify BEGIN and COMMIT are also properly skipped
	conn := &Conn{
		p: &PluginParam{
			AutoCreateTable: true,
			BatchSize:       500,
			Data:            NewTableData(),
		},
	}

	conn.p.Data.Data = append(conn.p.Data.Data, &pluginDriver.PluginDataType{
		EventType:  "insert",
		SchemaName: "test_db",
		TableName:  "test_table",
		Rows:       []map[string]interface{}{{"id": 1}},
	})

	for _, query := range []string{"BEGIN", "begin", "COMMIT", "commit"} {
		data := &pluginDriver.PluginDataType{Query: query}
		lastSuccess, errData, err := conn.Query(data, false)
		if lastSuccess != nil || errData != nil || err != nil {
			t.Errorf("Query(%q): expected all nil returns, got lastSuccess=%v errData=%v err=%v", query, lastSuccess, errData, err)
		}
		if len(conn.p.Data.Data) == 0 {
			t.Errorf("Query(%q): pending data was flushed", query)
		}
	}
}

func TestQuery_AutoCreateTableFalse(t *testing.T) {
	// When AutoCreateTable is false, ALL queries should return nil without
	// affecting the data buffer.
	conn := &Conn{
		p: &PluginParam{
			AutoCreateTable: false,
			BatchSize:       500,
			Data:            NewTableData(),
		},
	}

	conn.p.Data.Data = append(conn.p.Data.Data, &pluginDriver.PluginDataType{
		EventType: "insert",
		Rows:      []map[string]interface{}{{"id": 1}},
	})

	for _, query := range []string{"INSERT INTO t VALUES (1)", "ALTER TABLE t ADD COLUMN c INT", "SAVEPOINT sp1"} {
		data := &pluginDriver.PluginDataType{Query: query}
		lastSuccess, _, err := conn.Query(data, false)
		if lastSuccess != nil || err != nil {
			t.Errorf("Query(%q) with AutoCreateTable=false: expected nil, got lastSuccess=%v err=%v", query, lastSuccess, err)
		}
		if len(conn.p.Data.Data) == 0 {
			t.Errorf("Query(%q) with AutoCreateTable=false: data was flushed", query)
		}
	}
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
