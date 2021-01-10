package src

import "testing"

import "github.com/brokercap/Bifrost/sdk/pluginTestData"

func TestConn_TranferQuerySql(t *testing.T) {
	p := &PluginParam{
		AutoTable:true,
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
}