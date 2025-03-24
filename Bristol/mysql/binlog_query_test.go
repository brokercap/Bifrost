package mysql

import "testing"

func TestEventParser_GetQueryTableName(t *testing.T) {
	parser := newEventParser(nil)
	var sql string
	var SchemaName, TableName string
	var noReloadTableInfo bool

	sql = `ALTER TABLE tableTestName
  ADD PRIMARY KEY (id),
  ADD UNIQUE KEY unique_code (unique_code) USING BTREE,
  ADD KEY gate_id (gate_id) USING BTREE,
  ADD KEY gate_auth_id (gate_auth_id) USING BTREE;`
	SchemaName, TableName, noReloadTableInfo, _ = parser.GetQueryTableName(sql)

	if SchemaName == "" && TableName == "tableTestName" && noReloadTableInfo == false {
		t.Log("alter tableTestName success")
	} else {
		t.Errorf("alter tableTestName error: %s", sql)
	}

	sql = `CREATE UNIQUE INDEX index_name ON tableTestName (column_name)`
	SchemaName, TableName, noReloadTableInfo, _ = parser.GetQueryTableName(sql)

	if SchemaName == "" && TableName == "tableTestName" && noReloadTableInfo == false {
		t.Log("CREATE UNIQUE INDEX tableTestName success")
	} else {
		t.Errorf("CREATE UNIQUE INDEX error: %s", sql)
	}

	sql = `CREATE  INDEX index_name ON tableTestName (column_name)`
	SchemaName, TableName, noReloadTableInfo, _ = parser.GetQueryTableName(sql)

	if SchemaName == "" && TableName == "tableTestName" && noReloadTableInfo == false {
		t.Log("CREATE INDEX tableTestName success")
	} else {
		t.Errorf("CREATE INDEX error: %s", sql)
	}

	sql = `TRUNCATE TABLE db.tableTestName`
	SchemaName, TableName, noReloadTableInfo, _ = parser.GetQueryTableName(sql)

	if SchemaName == "db" && TableName == "tableTestName" && noReloadTableInfo == false {
		t.Log("TRUNCATE TABLE tableTestName success")
	} else {
		t.Errorf("TRUNCATE TABLE error: %s", sql)
	}

	sql = `TRUNCATE  testTableName`
	SchemaName, TableName, noReloadTableInfo, _ = parser.GetQueryTableName(sql)

	if SchemaName == "" && TableName == "testTableName" && noReloadTableInfo == false {
		t.Log("TRUNCATE testTableName success")
	} else {
		t.Errorf("TRUNCATE error: %s", sql)
	}
}

func TestTransferNotes2Space(t *testing.T) {
	var sql string
	sql = "rename /* gh-ost */ table `mydb`.`tab1` to `mydb`.`_tab1_del`, `mydb`.`_tab1_gho` to `mydb`.`tab1`"
	sql = TransferNotes2Space(sql)
	t.Log(sql)

	sql = "rename /* " +
		"gh-ost */ table `mydb`.`tab1` to `mydb`.`_tab1_del`, `mydb`.`_tab1_gho` to `mydb`.`tab1`"
	sql = TransferNotes2Space(sql)
	t.Log(sql)
}
