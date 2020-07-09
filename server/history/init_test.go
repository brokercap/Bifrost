package history

import "testing"

func TestHistory_Sync(t *testing.T) {
	history := History{
		ID:         1,
		DbName:     "sisilily",
		SchemaName: "sisilily",
		TableName:  "cc_products",
		Status:     HISTORY_STATUS_CLOSE,
		NowStartI:  0,
		Property: HistoryProperty{
			ThreadNum:      1,
			ThreadCountPer: 1000,
			SyncThreadNum:  1,
			LimitOptimize:  1,
		},
		ToServerIDList: []int{1},
		ThreadPool:     make([]*ThreadStatus, 0),
		Uri:            "root:Root@163.@tcp(47.90.43.60:3307)/sisilily",
	}

	history.Start()

}
