package history_test

import "testing"
import "github.com/jc3wish/Bifrost/server/history"

func TestGetDataList(t *testing.T)  {
	historyObj := &history.History{
		DbName:"test",
		SchemaName:"bifrost_test",
		TableName:"bristol_performance_test",
		Status:history.CLOSE,
		NowStartI:0,
		Property:history.HistoryProperty{
			ThreadNum:1,
			ThreadCountPer:10,
		},
		Uri:"root:@tcp(127.0.0.1:3306)/test",
	}
	historyObj.Start();
}
