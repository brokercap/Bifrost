package history

import (
	"fmt"
	"github.com/brokercap/Bifrost/Bristol/mysql"
	"github.com/brokercap/Bifrost/server"
	"github.com/robfig/cron/v3"
	"log"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

var historyMap map[string]map[int]*History

var lastHistoryID int

var l sync.RWMutex

func init() {
	lastHistoryID = 0
	historyMap = make(map[string]map[int]*History, 0)
}

type HisotryStatus string

const (
	HISTORY_STATUS_ALL            HisotryStatus = "All"
	HISTORY_STATUS_CLOSE          HisotryStatus = "close"
	HISTORY_STATUS_RUNNING        HisotryStatus = "running"
	HISTORY_STATUS_OVER           HisotryStatus = "over"
	HISTORY_STATUS_HALFWAY        HisotryStatus = "halfway"
	HISTORY_STATUS_KILLED         HisotryStatus = "killed"
	HISTORY_STATUS_SELECT_OVER    HisotryStatus = "selectOver" //拉取数据结束
	HISTORY_STATUS_SELECT_STOPING HisotryStatus = "stoping"
	HISTORY_STATUS_SELECT_STOPED  HisotryStatus = "stoped"
)

func AddHistory(dbName string, SchemaName string, TableName string, TableNames string, Property HistoryProperty, ToServerIDList []int) (int, error) {
	l.Lock()
	defer l.Unlock()
	db := server.GetDBObj(dbName)
	if db == nil {
		return 0, fmt.Errorf("%s not exist", dbName)
	}
	if _, ok := historyMap[dbName]; !ok {
		historyMap[dbName] = make(map[int]*History, 0)
	}
	if Property.SyncThreadNum <= 0 {
		Property.SyncThreadNum = 1
	}
	Property.FirstLimitOptimize = Property.LimitOptimize
	if len(ToServerIDList)*int(Property.SyncThreadNum) > 16384 {
		return 0, fmt.Errorf("SyncThreadNum * len(ToServerIDList) > 16384")
	}
	ID := lastHistoryID + 1
	TableNameArrTmp := strings.Split(TableNames, ";")
	TableNameArr := make([]*TableStatus, 0)
	for _, v := range TableNameArrTmp {
		if v == "" {
			continue
		}
		TableNameArr = append(TableNameArr, &TableStatus{RowsCount: 0, SelectCount: 0, TableName: strings.Trim(v, "")})
	}
	historyJob := &History{
		ID:                ID,
		DbName:            dbName,
		SchemaName:        SchemaName,
		TableName:         TableName,
		TableNames:        TableNames,
		TableNameArr:      TableNameArr,
		TableCount:        len(TableNameArr),
		TableCountSuccess: 0,
		CurrentTableName:  "",
		Status:            HISTORY_STATUS_CLOSE,
		NowStartI:         0,
		Property:          Property,
		ToServerIDList:    ToServerIDList,
		ThreadPool:        make([]*ThreadStatus, 0),
		Uri:               db.ConnectUri,
	}
	lastHistoryID = ID
	if Property.Crontab != "" {
		err := startCrond(historyJob)
		if err != nil {
			return 0, err
		}
	}
	historyMap[dbName][ID] = historyJob
	return ID, nil
}

func DelHistory(dbName string, ID int) bool {
	l.Lock()
	defer l.Unlock()
	if _, ok := historyMap[dbName]; !ok {
		return true
	}
	_ = deleteCrond(historyMap[dbName][ID])
	delete(historyMap[dbName], ID)
	if len(historyMap[dbName]) == 0 {
		delete(historyMap, dbName)
	}
	return true
}

func KillHistory(dbName string, ID int) error {
	l.Lock()
	defer l.Unlock()
	if _, ok := historyMap[dbName]; !ok {
		return fmt.Errorf("%s not exist", dbName)
	}
	if _, ok := historyMap[dbName][ID]; !ok {
		return fmt.Errorf("%s %d not exist", dbName, ID)
	}
	historyMap[dbName][ID].Status = HISTORY_STATUS_KILLED
	for _, toServer := range historyMap[dbName][ID].ToServerList {
		toServer.ToServerInfo.Status = "deled"
	}
	return nil
}

func StopHistory(dbName string, ID int) error {
	l.Lock()
	defer l.Unlock()
	if _, ok := historyMap[dbName]; !ok {
		return fmt.Errorf("%s not exist", dbName)
	}
	if _, ok := historyMap[dbName][ID]; !ok {
		return fmt.Errorf("%s %d not exist", dbName, ID)
	}
	historyMap[dbName][ID].Status = HISTORY_STATUS_SELECT_STOPING
	return nil
}

func GetHistoryList(dbName, SchemaName, TableName string, status HisotryStatus) []History {
	l.RLock()
	defer l.RUnlock()
	data := make([]History, 0)
	for dbNameKey, v := range historyMap {
		if dbName != "" {
			if dbNameKey != dbName {
				continue
			}
		}

		for _, historyInfo := range v {
			if SchemaName != "" {
				if SchemaName != historyInfo.SchemaName {
					continue
				}
				if TableName != "" {
					if TableName != historyInfo.TableName {
						continue
					}
				}
			}
			if status != HISTORY_STATUS_ALL {
				if historyInfo.Status != status {
					continue
				}
			}
			if historyInfo.cronEntryID > 0 {
				historyInfo.ContabNextTime = crodObj.Entry(historyInfo.cronEntryID).Next
			}
			data = append(data, *historyInfo)
		}
	}
	return data
}

func startCrond(job *History) error {
	if job == nil {
		return nil
	}
	job.Lock()
	defer job.Unlock()
	EntryID, err := crodObj.AddJob(job.Property.Crontab, job)
	if err != nil {
		log.Printf("[ERROR] history add crontab job DbName:%s SchemaName:%s ID:%d Crontab  err:%+v \n", job.DbName, job.SchemaName, job.ID, job.Property.Crontab, err)
		return err
	}
	job.cronEntryID = EntryID
	job.cronStatus = HISTORY_STATUS_RUNNING
	return nil
}

func deleteCrond(job *History) error {
	if job == nil {
		return nil
	}
	job.Lock()
	defer job.Unlock()
	if job.cronEntryID != 0 {
		crodObj.Remove(job.cronEntryID)
		job.cronEntryID = 0
		job.cronStatus = HISTORY_STATUS_CLOSE
	}
	return nil
}

type HistoryProperty struct {
	ThreadNum          int    // 拉取数据协程数量,每个协程一个连接
	ThreadCountPer     int    // 协程每次最多处理多少条数据
	Where              string // where 条件
	LimitOptimize      int8   // 是否自动分页优化, 1 采用 between 方式优化 0 不启动优化
	SyncThreadNum      int    // 同步协程数
	FirstLimitOptimize int8   // 被添加的时候 LimitOptimize 的值，因为计算的时候，LimitOptimize 是可能被修改掉值
	Crontab            string // 定时表达式，如果为空，则说明没有定时
}

type ThreadStatus struct {
	Num       int
	Error     error  // 拉取数据错误
	NowStartI uint64 // 当前执行第几条
}

type toServer struct {
	sync.RWMutex
	threadCount  int
	ToServerInfo *server.ToServer
}

type WaitGroup struct {
	sync.RWMutex
	newAddCount int            // 需要新 add 的数量
	waitGroup   sync.WaitGroup // waitGroup
}

func NewWaitGroup(n int) *WaitGroup {
	w := &WaitGroup{newAddCount: 0, waitGroup: sync.WaitGroup{}}
	w.waitGroup.Add(n)
	return w
}

func (This *WaitGroup) Add(n int) {
	This.Lock()
	This.newAddCount += n
	This.Unlock()
}

func (This *WaitGroup) Wait() {
	This.Lock()
	if This.newAddCount > 0 {
		This.waitGroup.Add(This.newAddCount)
		This.newAddCount = 0
	}
	This.Unlock()
	This.waitGroup.Wait()
}

func (This *WaitGroup) Done() {
	This.Lock()
	defer This.Unlock()
	if This.newAddCount > 0 {
		This.newAddCount -= 1
		return
	}
	This.waitGroup.Done()
}

type TableStatus struct {
	sync.RWMutex
	RowsCount   uint64
	SelectCount uint64
	TableName   string
}

type History struct {
	sync.RWMutex
	ID                 int
	DbName             string
	SchemaName         string
	TableName          string
	Property           HistoryProperty
	Status             HisotryStatus
	NowStartI          uint64 //当前第几条数据
	ThreadPool         []*ThreadStatus
	threadResultChan   chan int        `json:"-"`
	Fields             []TableStruct   `json:"-"`
	TableInfo          TableInfoStruct `json:"-"`
	Uri                string          `json:"-"`
	ToServerIDList     []int
	StartTime          string
	OverTime           string
	TablePriKeyMinId   uint64 // 假如主键是自增id的情况下 这个值是当前自增id最小值
	TablePriKeyMaxId   uint64 // 假如主键是自增id的情况下 这个值是当前自增id最大值
	TablePriKey        string // 主键字段
	TablePriArr        []string
	ToServerList       []*toServer
	ToServerTheadCount int16 // 实际正在运行的同步协程数
	ToServerTheadGroup *WaitGroup
	TableNames         string            // 用 ; 隔开的表名
	TableNameArr       []*TableStatus    // TableNames 分割后的数组
	CurrentTableName   string            // 正在执行全量的表名
	TableCount         int               // 要全量的总表数量
	TableCountSuccess  int               // 已经成功的表数量
	selectStatus       bool              // 拉数据协程状态，true 为已拉完
	SelectRowsCount    uint64            // 成功拉取多少条数据
	ColumnMapping      map[string]string // 表字段类型

	cronEntryID    cron.EntryID  // 定时任务模块返回的ID
	cronStatus     HisotryStatus // 定时任务是否启动
	ContabNextTime time.Time     // 定时任务下一次运行时间

}

func Start(dbName string, ID int) error {
	if _, ok := historyMap[dbName]; !ok {
		return fmt.Errorf("%s not exist", dbName)
	}
	if _, ok := historyMap[dbName][ID]; !ok {
		return fmt.Errorf("%s %d not exist", dbName, ID)
	}
	return historyMap[dbName][ID].Start()
}

func (This *History) Run() {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
		}
	}()
	// 这里判断一次任务是否存在定时任务ID
	// 主要是防止在删除任务的时候，停掉定时任务时候因为异常导致，任务没被真实停掉，但是任务已经从界面上删除，导致任务还在后台运行等情况
	This.Lock()
	if This.cronEntryID == 0 {
		This.Unlock()
		return
	}
	This.Unlock()
	_ = This.Start()
}

func (This *History) LogError(errContent string) {
	log.Printf("[ERROR] history task ID:%d DbName:%s SchemaName:%s Table:%s ToServerIDList:%+v %s \n", This.ID, This.DbName, This.SchemaName, This.TableNames, This.ToServerIDList, errContent)
}

func (This *History) LogInfo(infoContent string) {
	log.Printf("[ERROR] history task ID:%d DbName:%s SchemaName:%s Table:%s ToServerIDList:%+v %s \n", This.ID, This.DbName, This.SchemaName, This.TableNames, This.ToServerIDList, infoContent)
}

func (This *History) Start() error {
	This.Lock()
	This.LogInfo("start")
	This.selectStatus = false
	switch This.Status {
	case HISTORY_STATUS_SELECT_STOPING:
		This.LogError("is stoping")
		return fmt.Errorf("is stoping")
		break
	case HISTORY_STATUS_RUNNING:
		This.Unlock()
		This.LogError("is running")
		return fmt.Errorf("is running")
		break
	case HISTORY_STATUS_SELECT_STOPED:
		break
	case HISTORY_STATUS_HALFWAY:
		This.NowStartI = 0
		break
	case HISTORY_STATUS_OVER, HISTORY_STATUS_SELECT_OVER:
		This.TableCountSuccess = 0
		This.NowStartI = 0
		break
	default:
		This.NowStartI = 0
		break
	}
	This.StartTime = time.Now().Format("2006-01-02 15:04:05")
	This.Status = HISTORY_STATUS_RUNNING
	This.NowStartI = 0
	This.SelectRowsCount = 0
	This.Fields = make([]TableStruct, 0)
	This.ThreadPool = make([]*ThreadStatus, This.Property.ThreadNum)
	This.threadResultChan = make(chan int, 1)
	This.ToServerList = make([]*toServer, 0)
	This.OverTime = ""
	This.Unlock()

	go func() {
		defer func() {
			This.Lock()
			defer This.Unlock()
			This.OverTime = time.Now().Format("2006-01-02 15:04:05")
			for _, v := range This.ThreadPool {
				if v.Error != nil {
					This.Status = HISTORY_STATUS_HALFWAY
				}
			}
			if len(This.ToServerList) > 0 {
				This.ToServerList = nil
			}
			if This.Status != HISTORY_STATUS_HALFWAY && This.Status != HISTORY_STATUS_OVER && This.Status != HISTORY_STATUS_SELECT_STOPING && This.Status != HISTORY_STATUS_SELECT_STOPED {
				This.Status = HISTORY_STATUS_SELECT_OVER
			}
			if This.SelectRowsCount == 0 {
				This.Status = HISTORY_STATUS_OVER
			}
			This.selectStatus = true
		}()
		for i, _ := range This.TableNameArr {
			This.TableNameArr[i].SelectCount = 0
		}
		for {
			This.CurrentTableName = This.TableNameArr[This.TableCountSuccess].TableName
			This.Lock()
			switch This.Status {
			case HISTORY_STATUS_HALFWAY, HISTORY_STATUS_SELECT_STOPING, HISTORY_STATUS_SELECT_STOPED, HISTORY_STATUS_KILLED:
				This.Unlock()
				return
			default:
				break
			}
			This.NowStartI = 0
			This.Unlock()
			var selectThreadWg sync.WaitGroup
			for i := 1; i <= This.Property.ThreadNum; i++ {
				selectThreadWg.Add(1)
				go This.threadStart(i-1, &selectThreadWg)
			}
			selectThreadWg.Wait()
			for _, v := range This.ThreadPool {
				if v.Error != nil {
					This.Status = HISTORY_STATUS_HALFWAY
				}
			}
			This.Lock()
			switch This.Status {
			case HISTORY_STATUS_HALFWAY, HISTORY_STATUS_SELECT_STOPING, HISTORY_STATUS_SELECT_STOPED, HISTORY_STATUS_KILLED:
				This.Unlock()
				return
			default:
				break
			}
			This.TableNameArr[This.TableCountSuccess].RowsCount = This.TableNameArr[This.TableCountSuccess].SelectCount
			This.TableCountSuccess++
			This.Unlock()
			if This.TableCountSuccess >= This.TableCount {
				break
			}
			This.Fields = make([]TableStruct, 0)
		}
	}()

	return nil
}

func (This *History) initMetaInfo(db mysql.MysqlConnection) {
	This.Lock()
	defer This.Unlock()
	if len(This.Fields) > 0 {
		return
	}
	This.TablePriKey = ""
	var isCk bool
	var err error
	if isCk, err = IsClickHouse(db); err != nil {
		This.LogError("check is clickhouse error")
		return
	}
	if !isCk {
		This.TableInfo = GetSchemaTableInfo(db, This.SchemaName, This.CurrentTableName)
	}

	//修改表记录总数，用于界面显示
	This.TableNameArr[This.TableCountSuccess].RowsCount = This.TableInfo.TABLE_ROWS

	This.Fields, err = GetSchemaTableFieldList(db, This.SchemaName, This.CurrentTableName, isCk)
	if err != nil {
		This.LogError(fmt.Sprintf("CurrentTableName:%s get schema table fields error:%+v ", This.CurrentTableName, err))
		return
	}
	This.TablePriArr = make([]string, 0)
	This.ColumnMapping = make(map[string]string, 0)
	for _, v := range This.Fields {
		if strings.ToUpper(*v.COLUMN_KEY) == "PRI" {
			This.TablePriArr = append(This.TablePriArr, *v.COLUMN_NAME)
		}
		var columnMappingType string
		switch *v.DATA_TYPE {
		case "tinyint":
			if strings.Index(*v.COLUMN_TYPE, "unsigned") >= 0 {
				columnMappingType = "uint8"
			} else {
				if *v.COLUMN_TYPE == "tinyint(1)" {
					columnMappingType = "bool"
				} else {
					columnMappingType = "int8"
				}
			}
		case "smallint":
			if strings.Index(*v.COLUMN_TYPE, "unsigned") >= 0 {
				columnMappingType = "uint16"
			} else {
				columnMappingType = "int16"
			}
		case "mediumint":
			if strings.Index(*v.COLUMN_TYPE, "unsigned") >= 0 {
				columnMappingType = "uint24"
			} else {
				columnMappingType = "int24"
			}
		case "int":
			if strings.Index(*v.COLUMN_TYPE, "unsigned") >= 0 {
				columnMappingType = "uint32"
			} else {
				columnMappingType = "int32"
			}
		case "bigint":
			if strings.Index(*v.COLUMN_TYPE, "unsigned") >= 0 {
				columnMappingType = "uint64"
			} else {
				columnMappingType = "int64"
			}
		case "numeric":
			columnMappingType = strings.Replace(*v.COLUMN_TYPE, "numeric", "decimal", 1)
		case "real":
			columnMappingType = strings.Replace(*v.COLUMN_TYPE, "real", "double", 1)
		case "Int8":
			columnMappingType = "int8"
		case "UInt8":
			columnMappingType = "uint8"
		case "Int16":
			columnMappingType = "int16"
		case "UInt16":
			columnMappingType = "uint16"
		case "Int32":
			columnMappingType = "int32"
		case "UInt32":
			columnMappingType = "uint32"
		case "Int64":
			columnMappingType = "int64"
		case "UInt64":
			columnMappingType = "uint64"
		case "Bool":
			columnMappingType = "bool"
		case "Float32":
			columnMappingType = "float"
		case "Float64":
			columnMappingType = "double"
		default:
			if strings.Contains(*v.COLUMN_TYPE, "Decimal") {
				columnMappingType = strings.Replace(*v.COLUMN_TYPE, "Decimal", "decimal", 1)
				break
			}
			if strings.Index(*v.COLUMN_TYPE, "Array") == 0 {
				columnMappingType = "json"
				break
			}
			if strings.Index(*v.COLUMN_TYPE, "Map") == 0 {
				columnMappingType = "json"
				break
			}
			columnMappingType = *v.COLUMN_TYPE
			break
		}
		if v.IS_NULLABLE != nil && *v.IS_NULLABLE != "NO" {
			columnMappingType = "Nullable(" + columnMappingType + ")"
		}
		This.ColumnMapping[*v.COLUMN_NAME] = columnMappingType
	}
	//假如只有一个主键并且主键自增的情况，找出这个主键最小值和最大值，只支持 无符号的数字。有符号的不支持
	if len(This.TablePriArr) > 0 {
		for _, v := range This.Fields {
			if strings.ToUpper(*v.COLUMN_KEY) == "PRI" && strings.ToLower(*v.EXTRA) == "auto_increment" {
				This.TablePriKeyMinId, This.TablePriKeyMaxId = GetTablePriKeyMinAndMaxVal(db, This.SchemaName, This.CurrentTableName, *v.COLUMN_NAME, This.Property.Where)
				This.TablePriKey = *v.COLUMN_NAME
				break
			}
		}
	}
	// 重新赋值在界面配置的 LimitOptimize 初始值
	This.Property.LimitOptimize = This.Property.FirstLimitOptimize
	// 没有主键的情况下,不能使用 between 等方式查询
	if This.TablePriKey == "" {
		This.Property.LimitOptimize = 0
	}
	// 当总数小于100万的时候的时候，并且自增id 最大值和最小值 差值 的分页数  是 直接 limit 分页数的 2 倍以上的时候，采用常规 limit 分页
	if This.Property.Where == "" && This.Property.LimitOptimize == 1 && This.TableInfo.TABLE_ROWS <= 1000000 && (This.TablePriKeyMaxId-This.TablePriKeyMinId)/uint64(This.Property.ThreadCountPer) > This.TableInfo.TABLE_ROWS/uint64(This.Property.ThreadCountPer)*2 {
		log.Println("history", This.DbName, This.SchemaName, This.CurrentTableName, This.ID, " TABLE_ROWS: ", This.TableInfo.TABLE_ROWS, " <= 1000000 ,then transfer LIMIT x,y")
		This.Property.LimitOptimize = 0
	}
	return
}
