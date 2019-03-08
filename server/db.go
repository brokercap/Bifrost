/*
Copyright [2018] [jc3wish]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package server

import (
	"sync"

	"github.com/jc3wish/Bifrost/Bristol/mysql"
	"log"
	"github.com/jc3wish/Bifrost/server/count"
)

var DbLock sync.Mutex

var DbList map[string]*db

func init() {
	DbList = make(map[string]*db, 0)
}

func AddNewDB(Name string, ConnectUri string, binlogFileName string, binlogPostion uint32, serverId uint32,maxFileName string,maxPosition uint32) *db {
	var r bool = false
	DbLock.Lock()
	if _, ok := DbList[Name]; !ok {
		DbList[Name] = NewDb(Name, ConnectUri, binlogFileName, binlogPostion, serverId,maxFileName,maxPosition)
		r = true
	}
	count.SetDB(Name)
	DbLock.Unlock()
	log.Println("Add db Info:",Name,ConnectUri,binlogFileName,binlogPostion,serverId,maxFileName,maxPosition)
	if r == true {
		return DbList[Name]
	} else {
		return nil
	}
}

func GetDBObj(Name string) *db{
	if _,ok := DbList[Name];!ok{
		return nil
	}
	return DbList[Name]
}


func DelDB(Name string) bool {
	DbLock.Lock()
	defer DbLock.Unlock()
	if _, ok := DbList[Name]; ok {
		if DbList[Name].ConnStatus == "close" {
			for _,c := range  DbList[Name].channelMap{
				count.DelChannel(Name,c.Name)
			}
			delete(DbList, Name)
			count.DelDB(Name)
			log.Println("delete db:",Name)
		} else {
			return false
		}
	}
	return true
}

type db struct {
	sync.Mutex
	Name               string `json:"Name"`
	ConnectUri         string `json:"ConnectUri"`
	ConnStatus         string `json:"ConnStatus"` //close,stop,starting,running
	ConnErr            string `json:"ConnErr"`
	channelMap         map[int]*Channel `json:"ChannelMap"`
	LastChannelID      int	`json:"LastChannelID"`
	tableMap           map[string]*Table `json:"TableMap"`
	binlogDump         *mysql.BinlogDump
	binlogDumpFileName string `json:"BinlogDumpFileName"`
	binlogDumpPosition uint32 `json:"BinlogDumpPosition"`
	replicateDoDb      map[string]uint8 `json:"ReplicateDoDb"`
	serverId           uint32 `json:"ServerId"`
	killStatus 			int
	maxBinlogDumpFileName string `json:"MaxBinlogDumpFileName"`
	maxBinlogDumpPosition uint32 `json:"MaxBinlogDumpPosition"`
}

type DbListStruct struct {
	Name               string
	ConnectUri         string
	ConnStatus         string //close,stop,starting,running
	ConnErr            string
	ChannelCount       int
	LastChannelID      int
	TableCount         int
	BinlogDumpFileName string
	BinlogDumpPosition uint32
	MaxBinlogDumpFileName string
	MaxBinlogDumpPosition uint32
	ReplicateDoDb      map[string]uint8
	ServerId           uint32
}

func GetListDb() map[string]DbListStruct {
	var dbListMap map[string]DbListStruct
	dbListMap = make(map[string]DbListStruct,0)
	DbLock.Lock()
	defer DbLock.Unlock()
	for k,v := range DbList{
		dbListMap[k] = DbListStruct{
			Name:v.Name,
			ConnectUri:v.ConnectUri,
			ConnStatus:v.ConnStatus,
			ConnErr:v.ConnErr,
			ChannelCount:len(v.channelMap),
			LastChannelID:v.LastChannelID,
			TableCount:len(v.tableMap),
			BinlogDumpFileName:v.binlogDumpFileName,
			BinlogDumpPosition:v.binlogDumpPosition,
			MaxBinlogDumpFileName:v.maxBinlogDumpFileName,
			MaxBinlogDumpPosition:v.maxBinlogDumpPosition,
			ReplicateDoDb:v.replicateDoDb,
			ServerId:v.serverId,
		}
	}
	return dbListMap
}


func NewDb(Name string, ConnectUri string, binlogFileName string, binlogPostion uint32, serverId uint32,maxFileName string,maxPosition uint32) *db {
	return &db{
		Name:               Name,
		ConnectUri:         ConnectUri,
		ConnStatus:         "close",
		ConnErr:            "",
		LastChannelID:		0,
		channelMap:         make(map[int]*Channel, 0),
		tableMap:           make(map[string]*Table, 0),
		binlogDumpFileName: binlogFileName,
		binlogDumpPosition: binlogPostion,
		maxBinlogDumpFileName:maxFileName,
		maxBinlogDumpPosition:maxPosition,
		binlogDump: &mysql.BinlogDump{
			DataSource:    ConnectUri,
			ReplicateDoDb: make(map[string]uint8, 0),
			OnlyEvent:     []mysql.EventType{mysql.WRITE_ROWS_EVENTv1, mysql.UPDATE_ROWS_EVENTv1, mysql.DELETE_ROWS_EVENTv1,mysql.WRITE_ROWS_EVENTv0, mysql.UPDATE_ROWS_EVENTv0, mysql.DELETE_ROWS_EVENTv0,mysql.WRITE_ROWS_EVENTv2, mysql.UPDATE_ROWS_EVENTv2, mysql.DELETE_ROWS_EVENTv2},
		},
		replicateDoDb: make(map[string]uint8, 0),
		serverId:      serverId,
		killStatus:			0,
	}
}
/*

func DelDb(Name string) error{
	DbLock.Lock()
	defer DbLock.Unlock()
	if _,ok := DbList[Name];!ok{
		return fmt.Errorf(Name+" not exsit")
	}
	if DbList[Name].ConnStatus == "close"{
		delete(DbList,Name)
		return nil
	}else{
		return fmt.Errorf(Name+" ConnStatus is not close")
	}
}
*/

func (db *db) SetServerId(serverId uint32) {
	db.serverId = serverId
}

func (db *db) SetReplicateDoDb(dbArr []string) bool {
	if db.ConnStatus == "close" || db.ConnStatus == "stop" {
		for i := 0; i < len(dbArr); i++ {
			db.replicateDoDb[dbArr[i]] = 1
		}
		db.binlogDump.ReplicateDoDb = db.replicateDoDb
		return true
	}
	return false
}

func (db *db) AddReplicateDoDb(dbName string) bool {
	db.Lock()
	defer db.Unlock()
	if _,ok:=db.replicateDoDb[dbName];!ok{
		db.replicateDoDb[dbName] = 1
	}
	return true
}

func (db *db) Start() (b bool) {
	b = false
	if db.maxBinlogDumpFileName == db.binlogDumpFileName && db.binlogDumpPosition >= db.maxBinlogDumpPosition{
		return
	}
	switch db.ConnStatus {
	case "close":
		db.ConnStatus = "starting"
		reslut := make(chan error, 1)
		db.binlogDump.CallbackFun = db.Callback

		go	db.binlogDump.StartDumpBinlog(db.binlogDumpFileName, db.binlogDumpPosition, db.serverId, reslut, db.maxBinlogDumpFileName, db.maxBinlogDumpPosition)

		go db.monitorDump(reslut)
		break
	case "stop":
		db.ConnStatus = "running"
		log.Println(db.Name+" monitor:","running")
		db.binlogDump.Start()
		break
	default:
		return
	}
	return true
}

func (db *db) Stop() bool {
	if db.ConnStatus == "running" {
		db.binlogDump.Stop()
		db.ConnStatus = "stop"
	}
	return true
}

func (db *db) Close() bool {
	db.ConnStatus = "closing"
	db.binlogDump.Close()
	return true
}

func (db *db) monitorDump(reslut chan error) bool {
	var lastStatus string = ""
	for {
		v := <-reslut
		if v.Error() != lastStatus{
			log.Println(db.Name+" monitor:",v.Error())
		}else{
			lastStatus = v.Error()
		}

		switch v.Error() {
		case "stop":
			db.ConnStatus = "stop"
			break
		case "running":
			db.ConnStatus = "running"
			db.ConnErr = "running"
			break
		default:
			db.ConnErr = v.Error()
			break
		}

		if v.Error() == "close" {
			db.ConnStatus = "close"
			break
		}
	}
	return true
}

func (db *db) AddTable(schemaName string, tableName string, ChannelKey int) bool {
	key := schemaName + "-" + tableName
	if _, ok := db.tableMap[key]; !ok {
		db.tableMap[key] = &Table{
			Name:         tableName,
			ChannelKey:   ChannelKey,
			ToServerList: make([]ToServer, 0),
		}
		log.Println("AddTable",db.Name,schemaName,tableName,db.channelMap[ChannelKey].Name)
		count.SetTable(db.Name,key)
	} else {
		db.Lock()
		db.tableMap[key].ChannelKey = ChannelKey
		db.Unlock()
	}
	return true
}

func (db *db) GetTable(schemaName string, tableName string) *Table {
	key := schemaName + "-" + tableName
	if _, ok := db.tableMap[key]; !ok {
		return  nil
	} else {
		return db.tableMap[key]
	}
}

func (db *db) DelTable(schemaName string, tableName string) bool {
	key := schemaName + "-" + tableName
	if _, ok := db.tableMap[key]; !ok {
		return true
	} else {
		db.Lock()
		delete(db.tableMap,key)
		db.Unlock()
		count.DelTable(db.Name,key)
		log.Println("DelTable",db.Name,schemaName,tableName)
	}
	return true
}

func (db *db) AddTableToServer(schemaName string, tableName string, toserver ToServer) bool {
	key := schemaName + "-" + tableName
	if _, ok := db.tableMap[key]; !ok {
		return false
	} else {
		db.Lock()
		db.tableMap[key].ToServerList = append(db.tableMap[key].ToServerList, toserver)
		db.Unlock()
		log.Println("AddTableToServer",db.Name,schemaName,tableName,toserver)
	}
	return true
}

func (db *db) DelTableToServer(schemaName string, tableName string, index int) bool {
	key := schemaName + "-" + tableName
	if _, ok := db.tableMap[key]; !ok {
		return false
	} else {
		db.Lock()
		if len(db.tableMap[key].ToServerList) < index-1{
			db.Unlock()
			return true
		}
		toServerInfo := db.tableMap[key].ToServerList[index]
		db.tableMap[key].ToServerList=append(db.tableMap[key].ToServerList[:index],db.tableMap[key].ToServerList[index+1:]...)
		db.Unlock()
		log.Println("DelTableToServer",db.Name,schemaName,tableName,"toServerInfo:",toServerInfo)
	}
	return true
}

func (db *db) AddChannel(Name string,MaxThreadNum int) *Channel {
	db.Lock()
	db.LastChannelID++
	ChannelID := db.LastChannelID
	if _, ok := db.channelMap[ChannelID]; ok {
		db.Unlock()
		return nil
	}
	c := NewChannel(MaxThreadNum,Name, db)
	db.channelMap[ChannelID] = c
	ch := count.SetChannel(db.Name,Name)
	db.channelMap[ChannelID].SetFlowCountChan(ch)
	db.Unlock()
	log.Println("AddChannel",db.Name,Name,"MaxThreadNum:",MaxThreadNum)
	return db.channelMap[ChannelID]
}

func (db *db) ListChannel() map[int]*Channel {
	db.Lock()
	defer  db.Unlock()
	return db.channelMap
}

func (db *db) GetChannel(channelID int) *Channel {
	if _,ok:=db.channelMap[channelID];!ok{
		return nil
	}
	return db.channelMap[channelID]
}

type ToServer struct {
	sync.RWMutex
	MustBeSuccess bool
	Type          string
	DataType      string //string , json
	AddEventType  bool
	AddSchemaName bool
	AddTableName  bool
	KeyConfig     string
	ValueConfig   string
	FieldList     []string
	ToServerKey   string
	Expir 		  int
	BinlogFileNum int
	BinlogPosition uint32
}

type Table struct {
	sync.Mutex
	Name         string
	ChannelKey   int
	ToServerList []ToServer
	//Plugin      string //*.so ,default:default.so
}

