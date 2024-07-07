package src

import (
	"hash/crc32"
	"runtime"
	"sync"
	"time"
)

type CountContent struct {
	Time        int64
	InsertCount uint64
	UpdateCount uint64
	DeleteCount uint64
	InsertRows  uint64
	UpdateRows  uint64
	DeleteRows  uint64
	DDLCount    uint64
	CommitCount uint64
}

func NewCountContent() *CountContent {
	return &CountContent{}
}

func ClearCountContent(c *CountContent) {
	c.Time = 0
	c.InsertCount = 0
	c.UpdateCount = 0
	c.DeleteCount = 0
	c.InsertRows = 0
	c.UpdateRows = 0
	c.DeleteRows = 0
	c.DDLCount = 0
	c.CommitCount = 0
}

type CountContentArr struct {
	Count   uint8
	Data    []CountContent
	Content *CountContent
}

type CountFlow struct {
	TenMinute *CountContentArr
	Hour      *CountContentArr
	EightHour *CountContentArr
	Day       *CountContentArr
	Content   *CountContent
}

func flowContentInit(n int) *CountContentArr {
	data := &CountContentArr{Data: make([]CountContent, 0), Content: NewCountContent()}
	return data
}

func NewFlow() *CountFlow {
	return &CountFlow{
		TenMinute: flowContentInit(120),
		Hour:      flowContentInit(120),
		EightHour: flowContentInit(96),
		Day:       flowContentInit(144),
		Content:   NewCountContent(),
	}
}

var cpuCount uint32 = uint32(runtime.NumCPU())

type dbMap struct {
	sync.RWMutex
	dbMap map[string]*db
}

type db struct {
	sync.RWMutex
	Name      string
	schemaMap map[string]map[string]*CountFlow
}

var dbArr []*dbMap

var crc_table *crc32.Table = crc32.MakeTable(0xD5828281)

func getDBArrI(dbname string) uint32 {
	return crc32.Checksum([]byte(dbname), crc_table) % cpuCount
}

func init() {
	dbArr = make([]*dbMap, runtime.NumCPU())
	for i := 0; i < runtime.NumCPU(); i++ {
		dbArr[i] = &dbMap{
			dbMap: make(map[string]*db, 0),
		}
	}

	go intervalFlowCount()
}

type eventType int8

const (
	INSERT eventType = 1
	UPDATE eventType = 2
	DELETE eventType = 3
	DDL    eventType = 4
	COMMIT eventType = 5
)

func AddCount(dbname, schameName, tableName string, event eventType, rowsCount int, eventCount bool) {
	i := getDBArrI(dbname)
	t := dbArr[i]
	t.Lock()
	defer t.Unlock()

	if _, ok := t.dbMap[dbname]; !ok {
		t.dbMap[dbname] = &db{
			Name:      dbname,
			schemaMap: make(map[string]map[string]*CountFlow, 0),
		}
	}
	db := t.dbMap[dbname]
	if _, ok := db.schemaMap[schameName]; !ok {
		db.schemaMap[schameName] = make(map[string]*CountFlow, 0)
	}

	if _, ok := db.schemaMap[schameName][tableName]; !ok {
		db.schemaMap[schameName][tableName] = NewFlow()
	}

	t1 := db.schemaMap[schameName][tableName]
	t1.Content.Time = time.Now().Unix()
	switch event {
	case INSERT:
		t1.Content.InsertRows += uint64(rowsCount)
		if eventCount {
			t1.Content.InsertCount += 1
		}
		break
	case UPDATE:
		t1.Content.UpdateRows += uint64(rowsCount)
		if eventCount {
			t1.Content.UpdateCount += 1
		}
		break
	case DELETE:
		t1.Content.DeleteRows += uint64(rowsCount)
		if eventCount {
			t1.Content.DeleteCount += 1
		}
		break
	case DDL:
		t1.Content.DDLCount += 1
		break
	case COMMIT:
		t1.Content.CommitCount += 1
	default:
		break
	}
}

type DoSlice struct {
	DoTenMinuteSlice bool
	DoHourSlice      bool
	DoEightHourSlice bool
	DoDaySlice       bool
	DoClear          bool
}

func intervalFlowCount() {
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	var NowTime int64
	var fori uint = 0
	var DoSliceObj *DoSlice
	DoSliceObj = &DoSlice{}
	for {
		select {
		case <-timer.C:
			NowTime = time.Now().Unix()
			NowTime = NowTime - NowTime%5
			fori++
			if fori%20 == 0 {
				DoSliceObj.DoClear = true
			} else {
				DoSliceObj.DoClear = false
			}
			DoSliceObj.DoTenMinuteSlice = true
			if fori%6 == 0 {
				DoSliceObj.DoHourSlice = true
			} else {
				DoSliceObj.DoHourSlice = false
			}
			if fori%60 == 0 {
				DoSliceObj.DoEightHourSlice = true
			} else {
				DoSliceObj.DoEightHourSlice = false
			}
			if fori%120 == 0 {
				fori = 0
				DoSliceObj.DoDaySlice = true
			} else {
				DoSliceObj.DoDaySlice = false
			}
			for _, dbList := range dbArr {
				doFlowCount(dbList, NowTime, DoSliceObj)
			}
			timer.Reset(5 * time.Second)
			break
		}
	}
}

func doFlowCount(dbList *dbMap, NowTime int64, DoSlice *DoSlice) {
	dbList.Lock()
	defer dbList.Unlock()
	for _, schemaList := range dbList.dbMap {
		for _, tableList := range schemaList.schemaMap {
			for _, tableInfo := range tableList {
				doTableFlowCount(tableInfo, NowTime, DoSlice)
			}
		}
	}
}

func clearFlowData(data []CountContent, NowTime int64, timeDiff int64, listCount int) []CountContent {
	var i = 0
	for k, Count := range data {
		if NowTime-Count.Time <= timeDiff {
			i = k
			break
		}
	}
	if i > 0 {
		data = data[i:]
	}
	return data
}

func doTableFlowCount(tableInfo *CountFlow, NowTime int64, DoSlice *DoSlice) {
	//假如 time == 0 则表示，最近没有数据进来
	if tableInfo.Content.Time == 0 {
		return
	}

	// 10 分钟数据

	tableInfo.TenMinute.Data = append(tableInfo.TenMinute.Data, CountContent{
		Time:        NowTime,
		InsertCount: tableInfo.Content.InsertCount,
		UpdateCount: tableInfo.Content.UpdateCount,
		DeleteCount: tableInfo.Content.DeleteCount,
		InsertRows:  tableInfo.Content.InsertRows,
		UpdateRows:  tableInfo.Content.UpdateRows,
		DeleteRows:  tableInfo.Content.DeleteRows,
		DDLCount:    tableInfo.Content.DDLCount,
		CommitCount: tableInfo.Content.CommitCount,
	})
	tableInfo.TenMinute.Count++
	if tableInfo.TenMinute.Count > 120 {
		tableInfo.TenMinute.Data = tableInfo.TenMinute.Data[1:]
	}
	if DoSlice.DoClear {
		tableInfo.TenMinute.Data = clearFlowData(tableInfo.TenMinute.Data, NowTime, 600, 120)
	}

	// 每小时数据
	tableInfo.Hour.Content.InsertCount += tableInfo.Content.InsertCount
	tableInfo.Hour.Content.UpdateCount += tableInfo.Content.UpdateCount
	tableInfo.Hour.Content.DeleteCount += tableInfo.Content.DeleteCount
	tableInfo.Hour.Content.InsertRows += tableInfo.Content.InsertRows
	tableInfo.Hour.Content.UpdateRows += tableInfo.Content.UpdateRows
	tableInfo.Hour.Content.DeleteRows += tableInfo.Content.DeleteRows
	tableInfo.Hour.Content.DDLCount += tableInfo.Content.DDLCount
	tableInfo.Hour.Content.CommitCount += tableInfo.Content.CommitCount

	if DoSlice.DoHourSlice == true {
		tableInfo.Hour.Data = append(tableInfo.Hour.Data, CountContent{
			Time:        NowTime,
			InsertCount: tableInfo.Hour.Content.InsertCount,
			UpdateCount: tableInfo.Hour.Content.UpdateCount,
			DeleteCount: tableInfo.Hour.Content.DeleteCount,
			InsertRows:  tableInfo.Hour.Content.InsertRows,
			UpdateRows:  tableInfo.Hour.Content.UpdateRows,
			DeleteRows:  tableInfo.Hour.Content.DeleteRows,
			DDLCount:    tableInfo.Hour.Content.DDLCount,
			CommitCount: tableInfo.Hour.Content.CommitCount,
		})
		tableInfo.Hour.Count++
		if tableInfo.Hour.Count > 120 {
			tableInfo.Hour.Data = tableInfo.Hour.Data[1:]
		}
		ClearCountContent(tableInfo.Hour.Content)
		if DoSlice.DoClear {
			tableInfo.Hour.Data = clearFlowData(tableInfo.Hour.Data, NowTime, 3600, 120)
		}
	}

	// 8小时数据
	tableInfo.EightHour.Content.InsertCount += tableInfo.Content.InsertCount
	tableInfo.EightHour.Content.UpdateCount += tableInfo.Content.UpdateCount
	tableInfo.EightHour.Content.DeleteCount += tableInfo.Content.DeleteCount
	tableInfo.EightHour.Content.InsertRows += tableInfo.Content.InsertRows
	tableInfo.EightHour.Content.UpdateRows += tableInfo.Content.UpdateRows
	tableInfo.EightHour.Content.DeleteRows += tableInfo.Content.DeleteRows
	tableInfo.EightHour.Content.DDLCount += tableInfo.Content.DDLCount
	tableInfo.EightHour.Content.CommitCount += tableInfo.Content.CommitCount

	if DoSlice.DoEightHourSlice == true {
		tableInfo.EightHour.Data = append(tableInfo.EightHour.Data, CountContent{
			Time:        NowTime,
			InsertCount: tableInfo.EightHour.Content.InsertCount,
			UpdateCount: tableInfo.EightHour.Content.UpdateCount,
			DeleteCount: tableInfo.EightHour.Content.DeleteCount,
			InsertRows:  tableInfo.EightHour.Content.InsertRows,
			UpdateRows:  tableInfo.EightHour.Content.UpdateRows,
			DeleteRows:  tableInfo.EightHour.Content.DeleteRows,
			DDLCount:    tableInfo.EightHour.Content.DDLCount,
			CommitCount: tableInfo.EightHour.Content.CommitCount,
		})
		tableInfo.EightHour.Count++
		if tableInfo.EightHour.Count > 96 {
			tableInfo.EightHour.Data = tableInfo.EightHour.Data[1:]
		}
		ClearCountContent(tableInfo.EightHour.Content)
		if DoSlice.DoClear {
			tableInfo.EightHour.Data = clearFlowData(tableInfo.EightHour.Data, NowTime, 28800, 96)
		}
	}

	// 24小时数据
	tableInfo.Day.Content.InsertCount += tableInfo.Content.InsertCount
	tableInfo.Day.Content.UpdateCount += tableInfo.Content.UpdateCount
	tableInfo.Day.Content.DeleteCount += tableInfo.Content.DeleteCount
	tableInfo.Day.Content.InsertRows += tableInfo.Content.InsertRows
	tableInfo.Day.Content.UpdateRows += tableInfo.Content.UpdateRows
	tableInfo.Day.Content.DeleteRows += tableInfo.Content.DeleteRows
	tableInfo.Day.Content.DDLCount += tableInfo.Content.DDLCount
	tableInfo.Day.Content.CommitCount += tableInfo.Content.CommitCount

	if DoSlice.DoDaySlice == true {
		tableInfo.Day.Data = append(tableInfo.Day.Data, CountContent{
			Time:        NowTime,
			InsertCount: tableInfo.Day.Content.InsertCount,
			UpdateCount: tableInfo.Day.Content.UpdateCount,
			DeleteCount: tableInfo.Day.Content.DeleteCount,
			InsertRows:  tableInfo.Day.Content.InsertRows,
			UpdateRows:  tableInfo.Day.Content.UpdateRows,
			DeleteRows:  tableInfo.Day.Content.DeleteRows,
			DDLCount:    tableInfo.Day.Content.DDLCount,
			CommitCount: tableInfo.Day.Content.CommitCount,
		})
		tableInfo.Day.Count++
		if tableInfo.Day.Count > 144 {
			tableInfo.Day.Data = tableInfo.Day.Data[1:]
		}
		ClearCountContent(tableInfo.Day.Content)
		if DoSlice.DoClear {
			tableInfo.Day.Data = clearFlowData(tableInfo.Day.Data, NowTime, 86400, 144)
		}
	}

	ClearCountContent(tableInfo.Content)

}
