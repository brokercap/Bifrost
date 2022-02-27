package src

import (
	"github.com/brokercap/Bifrost/sdk/pluginTestData"
	"testing"
)

func TestConn_TranferQuerySql(t *testing.T) {
	/*	ckObj := &Conn{
		p: &PluginParam{
			CkSchema: "",
		},
	}*/

	var SchemaName, TableName, newSql string

	e := pluginTestData.NewEvent()

	queryEvent := e.GetTestQueryData()

	var sql string
	sql = `ALTER TABLE /* it is notes */ binlog_field_test 
  CHANGE testtinyint testtinyint INT UNSIGNED DEFAULT -1  NOT NULL,
  CHANGE testvarchar testvarchar VARCHAR(60) CHARSET utf8 COLLATE utf8_general_ci NOT NULL,
  ADD COLUMN testint2 INT(11) DEFAULT 0  NOT NULL   COMMENT 'test ok' AFTER test_json,
  MODIFY COLUMN testint3 int DEFAULT 1 NULL comment 'sdfsdf sdf',`

	queryEvent.Query = sql

	//SchemaName, TableName, newSql = ckObj.TranferQuerySql(queryEvent)

	t.Log(SchemaName, TableName, newSql)

	sql = "ALTER TABLE ppospro_gate_device_auth2 MODIFY COLUMN id7 BIGINT(20) DEFAULT 0;"
	queryEvent.Query = sql

	//SchemaName, TableName, newSql = ckObj.TranferQuerySql(queryEvent)

	t.Log(SchemaName, TableName, newSql)

	sql = "alter table binlog_field_test_7 add  col1 int;"
	queryEvent.Query = sql
	//SchemaName, TableName, newSql = ckObj.TranferQuerySql(queryEvent)

	t.Log(SchemaName, TableName, newSql)
}
