//go:build integration
// +build integration

package history

import (
	"testing"
)

func TestHistory_GetNextSql(t *testing.T) {
	historyObj := &History{
		DbName:     "test",
		SchemaName: "test",
		TableName:  "binlog_field_test",
		Status:     HISTORY_STATUS_CLOSE,
		NowStartI:  0,
		Property: HistoryProperty{
			ThreadNum:      1,
			ThreadCountPer: 10000,
			LimitOptimize:  1,
		},
		Uri: "root:root@tcp(192.168.220.128:3307)/bifrost_test",
	}
	var sql string
	var start uint64
	db := DBConnect(historyObj.Uri)
	historyObj.initMetaInfo(db)
	for {
		sql, start = historyObj.GetNextSql()
		t.Log("sql1:", sql)
		t.Log("start:", start)
		if sql == "" {
			break
		}
	}

	historyObj = &History{
		DbName:     "test",
		SchemaName: "test",
		TableName:  "binlog_field_test",
		Status:     HISTORY_STATUS_CLOSE,
		NowStartI:  0,
		Property: HistoryProperty{
			ThreadNum:      1,
			ThreadCountPer: 1000,
			Where:          " id > 3 ",
			LimitOptimize:  0,
		},
		Uri: "root:root@tcp(192.168.220.128:3307)/bifrost_test",
	}

	db = DBConnect(historyObj.Uri)
	historyObj.initMetaInfo(db)
	for {
		sql, start = historyObj.GetNextSql()
		t.Log("sql1:", sql)
		t.Log("start:", start)
		if sql == "" {
			break
		}
	}

}
