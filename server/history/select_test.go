package history

import (
	"testing"
)

func TestHistory_GetNextSql(t *testing.T) {
	historyObj := &History{
		DbName:"test",
		SchemaName:"bifrost_test",
		TableName:"binlog_field_test",
		Status:HISTORY_STATUS_CLOSE,
		NowStartI:0,
		Property:HistoryProperty{
			ThreadNum:1,
			ThreadCountPer:1,
		},
		Uri:"root:@tcp(127.0.0.1:3306)/bifrost_test",
	}

	db := DBConnect(historyObj.Uri)
	historyObj.initMetaInfo(db)
	for{
		sql := historyObj.GetNextSql()
		t.Log("sql1:",sql)
		if sql == ""{
			break
		}
	}

	historyObj = &History{
		DbName:"test",
		SchemaName:"bifrost_test",
		TableName:"binlog_field_test",
		Status:HISTORY_STATUS_CLOSE,
		NowStartI:0,
		Property:HistoryProperty{
			ThreadNum:1,
			ThreadCountPer:10,
			Where:" id > 0 ",
		},
		Uri:"root:@tcp(127.0.0.1:3306)/bifrost_test",
	}

	db = DBConnect(historyObj.Uri)
	historyObj.initMetaInfo(db)
	for{
		sql := historyObj.GetNextSql()
		t.Log("sql1:",sql)
		if sql == ""{
			break
		}
	}

}
