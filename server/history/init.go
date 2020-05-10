package history

import (
	"fmt"
	"sync"
	"github.com/brokercap/Bifrost/Bristol/mysql"
	"log"
	"time"
	"strings"
	"github.com/brokercap/Bifrost/server"
)

var historyMap map[string]map[int]*History

var lastHistoryID int

var l sync.RWMutex

func init()  {
	lastHistoryID = 0
	historyMap = make(map[string]map[int]*History,0)
}

type HisotryStatus string

const (
	HISTORY_STATUS_ALL 		HisotryStatus = "All"
	HISTORY_STATUS_CLOSE 	HisotryStatus = "close"
	HISTORY_STATUS_RUNNING	HisotryStatus = "running"
	HISTORY_STATUS_OVER		HisotryStatus = "over"
	HISTORY_STATUS_HALFWAY	HisotryStatus = "halfway"
	HISTORY_STATUS_KILLED	HisotryStatus = "killed"
	HISTORY_STATUS_SELECT_OVER	HisotryStatus = "selectOver"   //拉取数据结束
)


func AddHistory(dbName string,SchemaName string,TableName string,Property HistoryProperty,ToServerIDList []int) (int,error){
	l.Lock()
	defer l.Unlock()
	db := server.GetDBObj(dbName)
	if db == nil{
		return 0,fmt.Errorf("%s not exist",dbName)
	}
	if _,ok := historyMap[dbName];!ok{
		historyMap[dbName] = make(map[int]*History,0)
	}
	if Property.SyncThreadNum <= 0 {
		Property.SyncThreadNum = 1
	}
	if len(ToServerIDList) * int(Property.SyncThreadNum) > 16384 {
		return 0,fmt.Errorf("SyncThreadNum * len(ToServerIDList) > 16384")
	}
	ID := lastHistoryID+1
	historyMap[dbName][ID] = &History{
		ID:ID,
		DbName:dbName,
		SchemaName:SchemaName,
		TableName:TableName,
		Status:HISTORY_STATUS_CLOSE,
		NowStartI:0,
		Property:Property,
		ToServerIDList:ToServerIDList,
		ThreadPool:make([]*ThreadStatus,0),
		Uri:db.ConnectUri,
	}
	return ID,nil
}

func DelHistory(dbName string,ID int) bool {
	l.Lock()
	defer l.Unlock()
	if _,ok := historyMap[dbName];!ok{
		return true
	}
	delete(historyMap[dbName],ID)
	if len(historyMap[dbName]) == 0{
		delete(historyMap,dbName)
	}
	return true
}


func KillHistory(dbName string,ID int) error {
	l.Lock()
	defer l.Unlock()
	if _,ok:=historyMap[dbName];!ok{
		return fmt.Errorf("%s not exist",dbName)
	}
	if _,ok:=historyMap[dbName][ID];!ok{
		return fmt.Errorf("%s %d not exist",dbName,ID)
	}
	historyMap[dbName][ID].Status = HISTORY_STATUS_KILLED
	for _,toServerInfo := range historyMap[dbName][ID].ToServerList{
		toServerInfo.Status = "deled"
	}
	return nil
}

func GetHistoryList(dbName,SchemaName,TableName string,status HisotryStatus) []History {
	l.RLock()
	defer l.RUnlock()
	data := make([]History,0)
	for dbNameKey,v := range historyMap{
		if dbName!=""{
			if dbNameKey != dbName{
				continue
			}
		}

		for _,historyInfo := range v{
			if SchemaName != ""{
				if SchemaName != historyInfo.SchemaName{
					continue
				}
				if TableName != ""{
					if TableName != historyInfo.TableName{
						continue
					}
				}
			}
			if status != HISTORY_STATUS_ALL{
				if historyInfo.Status != status{
					continue
				}
			}
			data = append(data,*historyInfo)
		}
	}
	return data
}

type HistoryProperty struct {
	ThreadNum			int      // 拉取数据协程数量,每个协程一个连接
	ThreadCountPer		int		 // 协程每次最多处理多少条数据
	Where				string   // where 条件
	LimitOptimize		int8	 // 是否自动分页优化, 1 采用 between 方式优化 0 不启动优化
	SyncThreadNum		int		 // 同步协程数
}

type ThreadStatus struct {
	Num					int
	Error				error      // 拉取数据错误
	NowStartI			uint64     // 当前执行第几条
}

type History struct {
	sync.RWMutex
	ID					int
	DbName 				string
	SchemaName			string
	TableName			string
	Property			HistoryProperty
	Status				HisotryStatus
	NowStartI			uint64 //当前第几条数据
	ThreadPool			[]*ThreadStatus
	threadResultChan	chan int `json:"-"`
	Fields				[]TableStruct `json:"-"`
	TableInfo			TableInfoStruct `json:"-"`
	Uri					string `json:"-"`
	ToServerIDList		[]int
	StartTime			string
	OverTime			string
	TablePriKeyMinId	uint64		// 假如主键是自增id的情况下 这个值是当前自增id最小值
	TablePriKeyMaxId	uint64		// 假如主键是自增id的情况下 这个值是当前自增id最大值
	TablePriKey			string		// 主键字段
	TablePriArr			[]*string
	ToServerList		[]*server.ToServer
	ToServerTheadCount	int16			// 实际正在运行的同步协程数
	toServerTheadCountChan chan int16	// 同步协程 开始或者结束,都会往这个chan里 +1,-1写数据.用于计算是不是所有同步协程都已结束
}

func Start(dbName string,ID int) error {
	if _,ok:=historyMap[dbName];!ok{
		return fmt.Errorf("%s not exist",dbName)
	}
	if _,ok:=historyMap[dbName][ID];!ok{
		return fmt.Errorf("%s %d not exist",dbName,ID)
	}
	return historyMap[dbName][ID].Start()
}

func (This *History) Start() error {
	log.Println("history start",This.DbName,This.SchemaName,This.TableName)
	This.Lock()
	if This.Status == HISTORY_STATUS_RUNNING{
		This.Unlock()
		return fmt.Errorf("running had")
	}
	This.StartTime = time.Now().Format("2006-01-02 15:04:05")
	This.Status = HISTORY_STATUS_RUNNING
	This.NowStartI = 0
	This.Unlock()
	This.Fields = make([]TableStruct,0)
	This.ThreadPool = make([]*ThreadStatus,This.Property.ThreadNum)
	This.threadResultChan = make(chan int,1)
	This.ToServerList = make([]*server.ToServer,0)
	This.OverTime = ""
	for i:=1;i<=This.Property.ThreadNum;i++{
		go This.threadStart(i-1)
	}
	go func() {
		c:=0
		for{
			if c == This.Property.ThreadNum{
				break
			}
			i := <- This.threadResultChan
			log.Println("history threadResultChan over:",i,This.DbName,This.SchemaName,This.TableName)
			c++
		}
		This.OverTime = time.Now().Format("2006-01-02 15:04:05")
		for _,v := range This.ThreadPool{
			if v.Error != nil{
				This.Status = HISTORY_STATUS_HALFWAY
			}
		}
		if This.Status != HISTORY_STATUS_HALFWAY && This.Status != HISTORY_STATUS_OVER {
			This.Status = HISTORY_STATUS_SELECT_OVER
		}
	}()
	return nil
}

func (This *History) Stop() error {
	This.Status = "killed"
	return nil
}

func (This *History) initMetaInfo(db mysql.MysqlConnection)  {
	This.Lock()
	defer This.Unlock()
	if len(This.Fields) > 0{
		return
	}
	This.TableInfo = GetSchemaTableInfo(db,This.SchemaName,This.TableName)
	This.Fields = GetSchemaTableFieldList(db,This.SchemaName,This.TableName)
	This.TablePriArr = make([]*string,0)
	for _,v := range This.Fields{
		if strings.ToUpper(*v.COLUMN_KEY) == "PRI"{
			This.TablePriArr = append(This.TablePriArr,v.COLUMN_NAME)
		}
	}
	//假如只有一个主键并且主键自增的情况，找出这个主键最小值和最大值，只支持 无符号的数字。有符号的不支持
	if len(This.TablePriArr) > 0{
		for _,v := range This.Fields{
			if strings.ToUpper(*v.COLUMN_KEY) == "PRI" && strings.ToLower(*v.EXTRA) == "auto_increment"{
				This.TablePriKeyMinId,This.TablePriKeyMaxId = GetTablePriKeyMinAndMaxVal(db,This.SchemaName,This.TableName,*v.COLUMN_NAME,This.Property.Where)
				This.TablePriKey = *v.COLUMN_NAME
				break
			}
		}
	}
	return
}
