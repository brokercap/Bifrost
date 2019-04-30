package history

import (
	"fmt"
	"sync"
	"strconv"
	"github.com/jc3wish/Bifrost/Bristol/mysql"
	"database/sql/driver"
	"github.com/jc3wish/Bifrost/util/dataType"
	pluginDriver "github.com/jc3wish/Bifrost/plugin/driver"
	"github.com/jc3wish/Bifrost/server"
	"github.com/jc3wish/Bifrost/config"
	"strings"
	"log"
	"time"
	"runtime/debug"
)

var historyMap map[string]map[int]*History

var lastHistoryID int

var l sync.RWMutex

func init()  {
	lastHistoryID = 0
	historyMap = make(map[string]map[int]*History,0)
}

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
		Status:CLOSE,
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
		return fmt.Errorf("%s %s not exist",dbName,ID)
	}

	return nil
}

func GetHistoryList(dbName,SchemaName,TableName string) []History {
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
			data = append(data,*historyInfo)
		}
	}
	return data
}

type HistoryProperty struct {
	ThreadNum			int      // 协程数量,每个协程一个连接
	ThreadCountPer		int		   // 协程每次最多处理多少条数据
}

type HisotryStatus string

const (
	CLOSE 		HisotryStatus = "close"
	RUNNING					  = "running"
	OVER					  = "over"
	HALFWAY					  = "halfway"
	KILLED					  = "killed"
)

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
		return fmt.Errorf("%s %s not exist",dbName,ID)
	}
	return historyMap[dbName][ID].Start()
}

func (This *History) Start() error {
	log.Println("history start",This.DbName,This.SchemaName,This.TableName)
	This.Lock()
	if This.Status == RUNNING{
		This.Unlock()
		return fmt.Errorf("running had")
	}
	This.StartTime = time.Now().Format("2006-01-02 15:04:05")
	This.Status = RUNNING
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
				This.Status = HALFWAY
			}
		}
		if This.Status != HALFWAY{
			This.Status = OVER
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
		ToServerInfo.ToServerChan.To <- pluginData
	}

	p := make([]driver.Value, 0)

	for {
		This.Lock()
		start = This.NowStartI
		This.NowStartI += This.Property.ThreadCountPer
		This.Unlock()
		sql := "select * from " + This.SchemaName + "." + This.TableName + " LIMIT " + strconv.Itoa(start) + "," + strconv.Itoa(This.Property.ThreadCountPer)
		log.Println(sql)
		stmt, err := db.Prepare(sql)
		This.ThreadPool[i].NowStartI = start
		if err != nil{
			This.ThreadPool[i].Error = err
			stmt.Close()
			return
		}
		rows, err := stmt.Query(p)
		if err != nil{
			This.ThreadPool[i].Error = err
			return
		}
		n := len(This.Fields)
		m := make(map[string]interface{}, n)
		for {
			if This.Status == KILLED{
				return
			}
			dest := make([]driver.Value, n, n)
			err := rows.Next(dest)
			if err != nil {
				break
			}
			for i, v := range This.Fields {
				if dest[i] == nil{
					m[v.COLUMN_NAME] = nil
					continue
				}
				switch v.COLUMN_TYPE {
				case "set":
					s :=  string(dest[i].([]byte))
					m[v.COLUMN_NAME] = strings.Split(s, ",")
					break
				default:
					m[v.COLUMN_NAME], _ = dataType.TransferDataType(dest[i].([]byte), v.ToDataType)
					break
				}
			}
		}
		rows.Close()
		stmt.Close()

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
	}
}