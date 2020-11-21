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
}