package src

import "testing"

import "github.com/brokercap/Bifrost/sdk/pluginTestData"

func TestConn_TranferQuerySql(t *testing.T) {
	p := &PluginParam{
		AutoTable: true,
	}

	e := pluginTestData.NewEvent()

	conn := &Conn{}
	conn.p = p
	queryEvent := e.GetTestQueryData()
	queryEvent.Query = "rename table `test3` to `test2`,`test2` TO `test4`"
	newSql := conn.TranferQuerySql(queryEvent)
	t.Log(newSql)

	queryEvent.Query = "RENAME    TABLE papa_trade_order3  TO  papa_trade_order    , time_test4  TO time_test3 "
	newSql = conn.TranferQuerySql(queryEvent)
	t.Log(newSql)

	queryEvent.Query = " INSERT     INTO   TableName (id,val) values (1,'2'),( 2,'3');"
	newSql = conn.TranferQuerySql(queryEvent)
	t.Log(newSql)

	queryEvent.Query = " UPDATE　　　  TableName SET val = '2' where id = 1 ;"
	newSql = conn.TranferQuerySql(queryEvent)
	t.Log(newSql)

	queryEvent.Query = " DELETE    FROM             TableName where id = 1 ;"
	newSql = conn.TranferQuerySql(queryEvent)
	t.Log(newSql)

	queryEvent.Query = " TRUNCATE TABLE db.tableTestName ;"
	newSql = conn.TranferQuerySql(queryEvent)
	t.Log(newSql)

	queryEvent.Query = " TRUNCATE db.tableTestName ;"
	newSql = conn.TranferQuerySql(queryEvent)
	t.Log(newSql)

	queryEvent.Query = `ALTER TABLE tableTestName
  ADD PRIMARY KEY (id),
  ADD UNIQUE KEY unique_code (unique_code) USING BTREE,
  ADD KEY gate_id (gate_id) USING BTREE,
  ADD KEY gate_auth_id (gate_auth_id) USING BTREE;`
	newSql = conn.TranferQuerySql(queryEvent)
	t.Log(newSql)

	queryEvent.Query = `CREATE UNIQUE INDEX index_name ON tableTestName (column_name)`
	newSql = conn.TranferQuerySql(queryEvent)
	t.Log(newSql)

	queryEvent.Query = `CREATE  INDEX index_name ON tableTestName(column_name)`
	newSql = conn.TranferQuerySql(queryEvent)
	t.Log(newSql)

	queryEvent.Query = "CREATE DATABASE /*!32312 IF NOT EXISTS*/ `bifrost_test`"
	newSql = conn.TranferQuerySql(queryEvent)
	t.Log(newSql)

	queryEvent.Query = "CREATE DATABASE `bifrost_test`"
	newSql = conn.TranferQuerySql(queryEvent)
	t.Log(newSql)

}

func TestConn_TranferDMLSql(t *testing.T) {
	var newSql []string

	p := &PluginParam{
		AutoTable: true,
	}
	conn := &Conn{}
	conn.p = p
	e := pluginTestData.NewEvent()
	queryEvent := e.GetTestQueryData()

	queryEvent.Query = "insert /* its is nots */ into tab values (1,2,3)"
	newSql = conn.TranferDMLSql(queryEvent)
	t.Log(newSql)

	queryEvent.Query = "insert /*its is nots */ into tab values (1,2,3)"
	newSql = conn.TranferDMLSql(queryEvent)
	t.Log(newSql)

	queryEvent.Query = "insert /*its is nots*/ into tab values (1,2,3)"
	newSql = conn.TranferDMLSql(queryEvent)
	t.Log(newSql)

	queryEvent.Query = "insert    /*its is nots*/  into tab values (1,2,3)"
	newSql = conn.TranferDMLSql(queryEvent)
	t.Log(newSql)

}

func TestConn_TranferSql_Rename(t *testing.T) {
	p := &PluginParam{
		AutoTable: true,
	}

	e := pluginTestData.NewEvent()

	conn := &Conn{}
	conn.p = p
	queryEvent := e.GetTestQueryData()

	var newSql []string
	queryEvent.Query = "rename table `mytest`.`ppospro_gate_device_auth` to `mytest`.`_ppospro_gate_device_auth_del`, `mytest`.`ppospro_gate_device_auth2` to `mytest`.`_ppospro_gate_device_auth2_del`"
	newSql = conn.TranferQuerySql(queryEvent)
	t.Log(newSql)

	conn.isTiDB = true
	newSql = conn.TranferQuerySql(queryEvent)
	for _, sql := range newSql {
		t.Log(sql)
	}
}
