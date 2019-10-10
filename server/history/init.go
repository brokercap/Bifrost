package history

import (
	"fmt"
	"sync"
	"strconv"
	"github.com/brokercap/Bifrost/Bristol/mysql"
	"database/sql/driver"
	"github.com/brokercap/Bifrost/util/dataType"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"github.com/brokercap/Bifrost/server"
	"github.com/brokercap/Bifrost/server/count"
	"github.com/brokercap/Bifrost/config"

	"strings"
	"log"
	"time"
	"runtime/debug"
	"unsafe"
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
	ThreadNum			int      // 协程数量,每个协程一个连接
	ThreadCountPer		int		   // 协程每次最多处理多少条数据
}

type ThreadStatus struct {
	Num					int
	Error				error
	NowStartI			int     // 当前执行第几条
}

type History struct {
	sync.RWMutex
	ID					int
	DbName 				string
	SchemaName			string
	TableName			string
	Property			HistoryProperty
	Status				HisotryStatus
	NowStartI			int //当前第几条数据
	ThreadPool			[]*ThreadStatus
	threadResultChan	chan int `json:"-"`
	Fields				[]TableStruct `json:"-"`
	Uri					string `json:"-"`
	ToServerIDList		[]int
	StartTime			string
	OverTime			string
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
			log.Println("thread over:",i)
			c++
		}
		This.OverTime = time.Now().Format("2006-01-02 15:04:05")
		for _,v := range This.ThreadPool{
			if v.Error != nil{
				This.Status = HISTORY_STATUS_HALFWAY
			}
		}
		if This.Status != HISTORY_STATUS_HALFWAY{
			This.Status = HISTORY_STATUS_OVER
		}
	}()
	return nil
}

func (This *History) Stop() error {
	This.Status = "killed"
	return nil
}

func (This *History) getMetaInfo(db mysql.MysqlConnection)  {
	This.Lock()
	defer This.Unlock()
	if len(This.Fields) > 0{
		return
	}
	This.Fields = GetSchemaTableFieldList(db,This.SchemaName,This.TableName)
	return
}

func (This *History) threadStart(i int)  {
	log.Println("threadStart start:",i,This.SchemaName,This.TableName)
	defer func() {
		log.Println("threadStart over:",i,This.SchemaName,This.TableName)
		This.threadResultChan <- i
		if err :=recover();err!=nil{
			This.ThreadPool[i].Error = fmt.Errorf( fmt.Sprint(err) + string(debug.Stack()) )
			log.Println("History threadStart:",fmt.Sprint(err) + string(debug.Stack()))
		}
	}()
	This.ThreadPool[i] = &ThreadStatus{
		Num:i+1,
		Error:nil,
		NowStartI:0,
	}
	db := DBConnect(This.Uri)
	This.getMetaInfo(db)
	if len(This.Fields) == 0{
		This.ThreadPool[i].Error = fmt.Errorf("Fields empty,%s %s %s "+This.DbName,This.SchemaName,This.TableName)
		log.Println("Fields empty",This.DbName,This.SchemaName,This.TableName)
		return
	}
	var start int

	var toServerList []*server.ToServer
	toServerList = make([]*server.ToServer,0)

	dbSouceInfo := server.GetDBObj(This.DbName)
	for _,toServerInfo := range dbSouceInfo.GetTable(This.SchemaName,This.TableName).ToServerList{
		for _,ID := range This.ToServerIDList{
			if ID == toServerInfo.ToServerID{
				toServerList = append(toServerList,toServerInfo)
				break
			}
		}
	}
	countChan := dbSouceInfo.GetChannel(dbSouceInfo.GetTable(This.SchemaName,This.TableName).ChannelKey).GetCountChan()
	CountKey := This.SchemaName + "-" + This.TableName
	var sendToServerResult = func(ToServerInfo *server.ToServer,pluginData *pluginDriver.PluginDataType)  {
		ToServerInfo.Lock()
		status := ToServerInfo.Status
		if status == "deling" || status == "deled"{
			ToServerInfo.Unlock()
			return
		}
		if status == ""{
			ToServerInfo.Status = "running"
		}
		if ToServerInfo.ToServerChan == nil{
			ToServerInfo.ToServerChan = &server.ToServerChan{
				To:     make(chan *pluginDriver.PluginDataType, config.ToServerQueueSize),
			}
			go ToServerInfo.ConsumeToServer(dbSouceInfo,pluginData.SchemaName,pluginData.TableName)
		}
		ToServerInfo.Unlock()
		ToServerInfo.ToServerChan.To <- pluginData
	}

	//sql := "select * from " + This.SchemaName + "." + This.TableName + " LIMIT " + strconv.Itoa(start) + "," + strconv.Itoa(This.Property.ThreadCountPer)
	/*
	sql := "select * from " + This.SchemaName + "." + This.TableName + " LIMIT ?,"+strconv.Itoa(This.Property.ThreadCountPer)
	stmt, err := db.Prepare(sql)
	if err != nil{
		log.Println("sssssssss")
		This.ThreadPool[i].Error = err
		stmt.Close()
		return
	}


	_,err := db.Exec("USE "+This.SchemaName ,[]driver.Value{})
	if err != nil{
		log.Println("use error:",err)
	}
	*/
	n := len(This.Fields)
	for {
		This.Lock()
		start = This.NowStartI
		This.NowStartI += This.Property.ThreadCountPer
		This.Unlock()
		sql := "select * from `"+This.SchemaName+"`.`"+This.TableName +"` LIMIT " + strconv.Itoa(start) + "," + strconv.Itoa(This.Property.ThreadCountPer)
		//sql := "select * from ? LIMIT ?,?"

		stmt, err := db.Prepare(sql)
		if err != nil{
			This.ThreadPool[i].Error = err
			log.Println("threadStart err:",err,"sql:",sql)
			return
		}
		This.ThreadPool[i].NowStartI = start
		p:=make([]driver.Value,0)
		/*
		p[0] = This.SchemaName+"."+This.TableName
		p[1] = strconv.Itoa(start)
		p[2] = strconv.Itoa(This.Property.ThreadCountPer)
		//p[0]=strconv.Itoa(start)
		*/
		rows, err := stmt.Query(p)
		if err != nil{
			This.ThreadPool[i].Error = err
			return
		}
		rowCount := 0
		for {
			if This.Status == HISTORY_STATUS_KILLED{
				return
			}
			dest := make([]driver.Value, n, n)
			err := rows.Next(dest)
			if err != nil {
				//log.Println("ssssssssff err:",err)
				break
			}
			rowCount++
			m := make(map[string]interface{}, n)
			sizeCount := int64(0)
			for i, v := range This.Fields {
				if dest[i] == nil{
					m[v.COLUMN_NAME] = nil
					continue
				}
				switch v.DATA_TYPE {
				case "set":
					s :=  string(dest[i].([]byte))
					m[v.COLUMN_NAME] = strings.Split(s, ",")
					break
				default:
					m[v.COLUMN_NAME], _ = dataType.TransferDataType(dest[i].([]byte), v.ToDataType)
					break
				}
				sizeCount += int64(unsafe.Sizeof(m[v.COLUMN_NAME]))
			}
			if len(m) == 0{
				return
			}
			Rows := make([]map[string]interface{},1)
			Rows[0] = m
			d := &pluginDriver.PluginDataType{
				Timestamp:		uint32(time.Now().Unix()),
				EventType: 		"insert",
				Rows:           Rows,
				Query:          "",
				SchemaName:		This.SchemaName,
				TableName:		This.TableName,
				BinlogFileNum:	0,
				BinlogPosition:	0,
			}

			for _,toServerInfo := range toServerList{
				sendToServerResult(toServerInfo,d)
			}

			countChan <- &count.FlowCount{
				//Time:"",
				Count:1,
				TableId:CountKey,
				ByteSize:sizeCount*int64(len(toServerList)),
			}
		}
		rows.Close()
		stmt.Close()

		if rowCount < This.Property.ThreadCountPer{
			return
		}
	}
}