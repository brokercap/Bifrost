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

	"github.com/brokercap/Bifrost/Bristol/mysql"
	"log"
	"github.com/brokercap/Bifrost/server/count"
	"github.com/brokercap/Bifrost/server/warning"
	"time"
	"strings"
	"strconv"
	"fmt"
	"runtime/debug"
	"regexp"
)

var dbAndTableSplitChars = "_-"

func GetSchemaAndTableJoin(schema,tableName string) string  {
	return schema + dbAndTableSplitChars + tableName
}

func GetSchemaAndTableBySplit(schemaAndTableName string) (schemaName,tableName string)  {
	var i int
	// 这里这么操作 是因为 最开始设计 的时候是用  - 分割，现在发现 有不少用户 库名也有 -
	// 为了兼容 ， 这里先判断一下 -, 是否存在，假如哪个用户 库名和表名都有 - 这个时候就会有问题了，但愿没这样的用户嘿嘿
	i = strings.Index(schemaAndTableName, dbAndTableSplitChars)
	if i == -1{
		if strings.Count(schemaAndTableName,"-") > 1{
			i = strings.LastIndexAny(schemaAndTableName, "-")
		}else{
			i = strings.IndexAny(schemaAndTableName, "-")
		}
		schemaName = schemaAndTableName[0:i]
		tableName = schemaAndTableName[i+1:]
	}else{
		schemaName = schemaAndTableName[0:i]
		tableName = schemaAndTableName[i+2:]
	}
	return
}

var AllSchemaAndTablekey string = GetSchemaAndTableJoin("*","*")

var DbLock sync.Mutex

var DbList map[string]*db

func init() {
	DbList = make(map[string]*db, 0)
}

func AddNewDB(Name string, ConnectUri string, binlogFileName string, binlogPostion uint32, serverId uint32,maxFileName string,maxPosition uint32,AddTime int64) *db {
	var r bool = false
	DbLock.Lock()
	if _, ok := DbList[Name]; !ok {
		DbList[Name] = NewDb(Name, ConnectUri, binlogFileName, binlogPostion, serverId,maxFileName,maxPosition,AddTime)
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

func UpdateDB(Name string, ConnectUri string, binlogFileName string, binlogPostion uint32, serverId uint32,maxFileName string,maxPosition uint32,UpdateTime int64,updateToServer int8) error {
	DbLock.Lock()
	defer DbLock.Unlock()
	if _, ok := DbList[Name]; !ok {
		return fmt.Errorf(Name + " not exsit")
	}
	if binlogFileName == ""{
		return fmt.Errorf("binlogFileName can't be empty")
	}
	if binlogPostion < 4{
		return fmt.Errorf("binlogPostion can't < 4")
	}
	if serverId == 0 {
		return fmt.Errorf("serverId can't be 0")
	}
	index := strings.IndexAny(binlogFileName,".")
	if index == -1{
		return fmt.Errorf("binlogFileName:%s error",binlogFileName)
	}
	dbObj := DbList[Name]
	dbObj.Lock()
	defer dbObj.Unlock()
	if dbObj.ConnStatus != "close"{
		return fmt.Errorf("db status must be close")
	}
	dbObj.ConnectUri = ConnectUri
	dbObj.binlogDumpFileName = binlogFileName
	dbObj.binlogDumpPosition = binlogPostion
	dbObj.serverId = serverId
	dbObj.maxBinlogDumpFileName = maxFileName
	dbObj.maxBinlogDumpPosition = maxPosition
	dbObj.AddTime = UpdateTime
	log.Println("Update db Info:",Name,ConnectUri,binlogFileName,binlogPostion,serverId,maxFileName,maxPosition)
	if updateToServer == 0{
		return nil
	}
	BinlogFileNum,_ := strconv.Atoi(binlogFileName[index+1:])
	for key,t := range dbObj.tableMap{
		for _,toServer:=range t.ToServerList{
			log.Println("UpdateToServerBinlogPosition:",key," QueueMsgCount:",toServer.QueueMsgCount," old:",toServer.BinlogFileNum,toServer.BinlogPosition," new:",BinlogFileNum,binlogPostion)
			toServer.UpdateBinlogPosition(BinlogFileNum,binlogPostion)
		}
	}
	return nil
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
	DBPositionBinlogKey := getDBBinlogkey(DbList[Name])
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
	// 删除binlog 信息
	delBinlogPosition(DBPositionBinlogKey)
	return true
}

type db struct {
	sync.RWMutex
	Name               		string `json:"Name"`
	ConnectUri         		string `json:"ConnectUri"`
	ConnStatus         		string `json:"ConnStatus"` //close,stop,starting,running
	ConnErr            		string `json:"ConnErr"`
	channelMap         		map[int]*Channel `json:"ChannelMap"`
	LastChannelID      		int	`json:"LastChannelID"`
	tableMap           		map[string]*Table `json:"TableMap"`
	binlogDump         		*mysql.BinlogDump
	binlogDumpFileName 		string `json:"BinlogDumpFileName"`
	binlogDumpPosition 		uint32 `json:"BinlogDumpPosition"`
	binlogDumpTimestamp 	uint32 `json:"BinlogDumpTimestamp"`
	replicateDoDb      		map[string]uint8 `json:"ReplicateDoDb"`
	serverId           		uint32 `json:"ServerId"`
	killStatus 				int
	maxBinlogDumpFileName 	string `json:"MaxBinlogDumpFileName"`
	maxBinlogDumpPosition 	uint32 `json:"MaxBinlogDumpPosition"`
	AddTime					int64
	DBBinlogKey				[]byte `json:"-"`  // 保存 binlog到levelDB 的key
}

type DbListStruct struct {
	Name               		string
	ConnectUri         		string
	ConnStatus         		string //close,stop,starting,running
	ConnErr            		string
	ChannelCount       		int
	LastChannelID      		int
	TableCount         		int
	BinlogDumpFileName 		string
	BinlogDumpPosition 		uint32
	BinlogDumpTimestamp		uint32
	MaxBinlogDumpFileName 	string
	MaxBinlogDumpPosition 	uint32
	ReplicateDoDb      		map[string]uint8
	ServerId           		uint32
	AddTime					int64
}

func GetListDb() map[string]DbListStruct {
	var dbListMap map[string]DbListStruct
	dbListMap = make(map[string]DbListStruct,0)
	DbLock.Lock()
	defer DbLock.Unlock()
	for k,v := range DbList{
		dbListMap[k] = DbListStruct{
			Name:					v.Name,
			ConnectUri:				v.ConnectUri,
			ConnStatus:				v.ConnStatus,
			ConnErr:				v.ConnErr,
			ChannelCount:			len(v.channelMap),
			LastChannelID:			v.LastChannelID,
			TableCount:				len(v.tableMap),
			BinlogDumpFileName:		v.binlogDumpFileName,
			BinlogDumpPosition:		v.binlogDumpPosition,
			BinlogDumpTimestamp:	v.binlogDumpTimestamp,
			MaxBinlogDumpFileName:	v.maxBinlogDumpFileName,
			MaxBinlogDumpPosition:	v.maxBinlogDumpPosition,
			ReplicateDoDb:			v.replicateDoDb,
			ServerId:				v.serverId,
			AddTime:				v.AddTime,
		}
	}
	return dbListMap
}


func GetDbInfo(dbname string) *DbListStruct {
	DbLock.Lock()
	defer DbLock.Unlock()
	v := DbList[dbname]
	if v == nil{
		return &DbListStruct{}
	}
	return &DbListStruct{
			Name:					v.Name,
			ConnectUri:				v.ConnectUri,
			ConnStatus:				v.ConnStatus,
			ConnErr:				v.ConnErr,
			ChannelCount:			len(v.channelMap),
			LastChannelID:			v.LastChannelID,
			TableCount:				len(v.tableMap),
			BinlogDumpFileName:		v.binlogDumpFileName,
			BinlogDumpPosition:		v.binlogDumpPosition,
			BinlogDumpTimestamp:	v.binlogDumpTimestamp,
			MaxBinlogDumpFileName:	v.maxBinlogDumpFileName,
			MaxBinlogDumpPosition:	v.maxBinlogDumpPosition,
			ReplicateDoDb:			v.replicateDoDb,
			ServerId:				v.serverId,
			AddTime:				v.AddTime,
		}
}

func NewDbByNull() *db {
	return &db{}
}

func NewDb(Name string, ConnectUri string, binlogFileName string, binlogPostion uint32, serverId uint32,maxFileName string,maxPosition uint32,AddTime int64) *db {
	return &db{
		Name:               	Name,
		ConnectUri:         	ConnectUri,
		ConnStatus:         	"close",
		ConnErr:            	"",
		LastChannelID:			0,
		channelMap:         	make(map[int]*Channel, 0),
		tableMap:           	make(map[string]*Table, 0),
		binlogDumpFileName: 	binlogFileName,
		binlogDumpPosition: 	binlogPostion,
		maxBinlogDumpFileName:	maxFileName,
		maxBinlogDumpPosition:	maxPosition,
		binlogDump: 			mysql.NewBinlogDump(
									ConnectUri,
									nil,
									[]mysql.EventType{
										mysql.WRITE_ROWS_EVENTv2, mysql.UPDATE_ROWS_EVENTv2, mysql.DELETE_ROWS_EVENTv2,
										mysql.QUERY_EVENT,
										mysql.WRITE_ROWS_EVENTv1, mysql.UPDATE_ROWS_EVENTv1, mysql.DELETE_ROWS_EVENTv1,
										mysql.WRITE_ROWS_EVENTv0, mysql.UPDATE_ROWS_EVENTv0, mysql.DELETE_ROWS_EVENTv0,
									},
									nil,nil),
		replicateDoDb: 			make(map[string]uint8, 0),
		serverId:      			serverId,
		killStatus:				0,
		AddTime:				AddTime,
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
		return true
	}
	return false
}

func (db *db) AddReplicateDoDb(schemaName,tableName string,doLock bool) bool {
	if doLock {
		db.Lock()
		defer db.Unlock()
	}
	if tableName == ""{
		return false
	}
	TransferLikeTableReqName := db.TransferLikeTableReq(tableName)
	if db.binlogDump != nil {
		db.binlogDump.AddReplicateDoDb(schemaName, TransferLikeTableReqName)
		log.Printf("AddReplicateDoDb dbName:%s ,schemaName:%s, tableName:%s , TransferLikeTableReq:%s ",db.Name,schemaName,tableName,TransferLikeTableReqName)
	}
	if _,ok:=db.replicateDoDb[schemaName];!ok{
		db.replicateDoDb[schemaName] = 1
	}
	return true
}

func (db *db) DelReplicateDoDb(schemaName,tableName string,doLock bool) bool {
	if doLock {
		db.Lock()
		defer db.Unlock()
	}
	if tableName == ""{
		return false
	}
	TransferLikeTableReqName := db.TransferLikeTableReq(tableName)
	if db.binlogDump != nil {
		db.binlogDump.DelReplicateDoDb(schemaName, TransferLikeTableReqName)
		log.Printf("DelReplicateDoDb dbName:%s ,schemaName:%s, tableName:%s , TransferLikeTableReq:%s ",db.Name,schemaName,tableName,TransferLikeTableReqName)

	}
	return true
}

func (db *db) TransferLikeTableReq(tableName string) string {
	if tableName == ""{
		return ""
	}
	var reqTableName string = tableName
	if tableName != "*" && strings.Index(tableName,"*") > -1 {
		if tableName[0:1] == "*" {
			reqTableName = "(.*)" + tableName[1:]
		}
		// 只要前面不是 （.*）,则自动替换面 ^ 开头，代表前面没数据了
		if strings.Index(reqTableName, "(.*)") != 0 && reqTableName[0:1] != "^" {
			reqTableName = "^" + reqTableName
		}
		var reqTablelen = len(reqTableName)
		// 假如末尾是 *，则替换面 (.*)
		// binlog_field_test_*  会匹配 出 binlog_field_test ，但是  binlog_field_test_（.*） 不会匹配 binlog_field_test 出来
		if reqTableName[reqTablelen-1:] == "*" {
			reqTableName = reqTableName[0:reqTablelen-1]+"(.*)"
		}
		// 字符串如果不是 (.*) 结尾,则自动替换面 $,代表后面没有数据了
		reqTablelen = len(reqTableName)
		if reqTablelen >= 4 && reqTableName[reqTablelen-4:] != "(.*)" && reqTableName[reqTablelen:] != "$" {
			reqTableName += "$"
		}
	}
	return reqTableName
}

func (db *db) getRightBinlogPosition() (newPosition uint32) {
	defer func() {
		if err := recover();err != nil{
			log.Println(db.Name," getRightBinlogPosition recover err:",err," binlogDumpFileName:",db.binlogDumpFileName," binlogDumpPosition:",db.binlogDumpPosition)
			log.Println(string(debug.Stack()))
			newPosition = 0
		}
	}()
	err := mysql.CheckBinlogIsRight(db.ConnectUri,db.binlogDumpFileName,db.binlogDumpPosition)
	if err == nil {
		return db.binlogDumpPosition
	}
	log.Println(db.Name," getRightBinlogPosition err:",err," binlogDumpFileName:",db.binlogDumpFileName," binlogDumpPosition:",db.binlogDumpPosition)
	if strings.Index(err.Error(),"connect: operation timed out") != -1 {
		return newPosition
	}
	newPosition = mysql.GetNearestRightBinlog(db.ConnectUri,db.binlogDumpFileName,db.binlogDumpPosition,db.serverId,db.getReplicateDoDbMap(),nil)
	return newPosition
}

func (db *db) getReplicateDoDbMap() map[string]map[string]uint8 {
	replicateDoDb:=make(map[string]map[string]uint8,0)
	for k,_ := range db.tableMap{
		schemaName,TableName := GetSchemaAndTableBySplit(k)
		if _,ok := replicateDoDb[schemaName];!ok{
			replicateDoDb[schemaName] = make(map[string]uint8,0)
		}
		replicateDoDb[schemaName][TableName] = 1
	}
	return replicateDoDb
}

func (db *db) Start() (b bool) {
	db.Lock()
	if db.ConnStatus != "close" && db.ConnStatus != "stop"{
		db.Unlock()
		return false
	}
	db.Unlock()
	b = false
	if db.maxBinlogDumpFileName == db.binlogDumpFileName && db.binlogDumpPosition >= db.maxBinlogDumpPosition{
		return
	}
	if len(db.tableMap) == 0{
		return
	}
	switch db.ConnStatus {
	case "close":
		db.ConnStatus = "starting"
		var newPosition uint32 = 0
		log.Println(db.Name,"Start(),and starting "," getRightBinlogPosition")
		for i:=0;i<3;i++{
			if db.ConnStatus == "closing"{
				break
			}
			newPosition = db.getRightBinlogPosition()
			if newPosition > 0{
				break
			}
			time.Sleep(time.Duration(1) * time.Second)
		}
		if db.ConnStatus == "closing"{
			db.ConnStatus = "close"
			db.ConnErr = "close"
			break
		}
		if newPosition == 0{
			/*
			db.ConnStatus = "close"
			db.ConnErr = "binlog position error"
			break
			*/
			log.Println("binlog poistion check failed,dbName:",db.Name,"current position:",db.binlogDumpFileName," ",db.binlogDumpPosition)
		}else{
			log.Println("binlog position change,dbName:", db.Name ," old:",db.binlogDumpFileName," ",db.binlogDumpPosition," new:",db.binlogDumpFileName," ",newPosition)
			db.binlogDumpPosition = newPosition
		}
		reslut := make(chan error, 1)
		db.binlogDump.CallbackFun = db.Callback
		for key,_ := range db.tableMap{
			schemaName,TableName := GetSchemaAndTableBySplit(key)
			db.AddReplicateDoDb(schemaName,TableName,false)
		}
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
	db.Lock()
	defer db.Unlock()
	if db.ConnStatus == "running" {
		db.binlogDump.Stop()
		db.ConnStatus = "stop"
	}
	return true
}

func (db *db) Close() bool {
	db.Lock()
	defer db.Unlock()
	if db.ConnStatus != "stop" && db.ConnStatus != "starting"{
		return true
	}
	db.ConnStatus = "closing"
	db.binlogDump.Close()
	return true
}

func (db *db) monitorDump(reslut chan error) (r bool) {
	var lastStatus string = ""
	timer := time.NewTimer( 3 * time.Second)
	defer timer.Stop()
	var i uint8 = 0
	for {
		select {
		case v := <-reslut:
			timer.Reset(3 * time.Second)
			switch v.Error() {
			case "stop":
				i = 0
				db.ConnStatus = "stop"
				break
			case "running":
				i = 0
				db.ConnStatus = "running"
				db.ConnErr = "running"
				warning.AppendWarning(warning.WarningContent{
					Type:   warning.WARNINGNORMAL,
					DbName: db.Name,
					Body:   " running; last status:" + lastStatus,
				})
				break
			case "starting":
				db.ConnStatus = "starting"
				break
			case "close":
				log.Println(db.Name+" monitor:", v.Error())
				db.ConnStatus = "close"
				db.ConnErr = "close"
				warning.AppendWarning(warning.WarningContent{
					Type:   warning.WARNINGERROR,
					DbName: db.Name,
					Body:   " closed",
				})
				return
			default:
				i++
				if i % 3 == 0 || strings.Index(v.Error(),"parseEvent err") != -1{
					i = 0
					warning.AppendWarning(warning.WarningContent{
						Type:   warning.WARNINGERROR,
						DbName: db.Name,
						Body:   " "+v.Error() + "; last status:" + lastStatus,
					})
				}
				db.ConnErr = v.Error()
				break
			}

			log.Println(db.Name+" monitor:", v.Error())
			if v.Error() != "starting"{
				lastStatus = v.Error()
			}

			break
		case <- timer.C:
			timer.Reset(3 * time.Second)
			db.saveBinlog()
			break
		}
	}
	return true
}

func (db *db) saveBinlog(){
	FileName,Position,Timestamp := db.binlogDump.GetBinlog()
	if FileName == ""{
		return
	}
	//db.Lock()
	//保存位点,这个位点在重启 配置文件恢复的时候
	//一个db有可能有多个channel，数据顺序不用担心，因为实际在重启的时候 会根据最小的 ToServerList 的位点进行自动替换
	db.binlogDumpFileName,db.binlogDumpPosition,db.binlogDumpTimestamp = FileName,Position,Timestamp
	if db.DBBinlogKey == nil{
		db.DBBinlogKey = getDBBinlogkey(db)
	}
	//db.Unlock()
	index := strings.IndexAny(FileName, ".")

	BinlogFileNum,_ := strconv.Atoi(FileName[index+1:])
	saveBinlogPosition(db.DBBinlogKey,BinlogFileNum,db.binlogDumpPosition)
}

func (db *db) AddTable(schemaName string, tableName string, ChannelKey int,LastToServerID int) bool {
	key := GetSchemaAndTableJoin(schemaName,tableName)
	db.Lock()
	defer db.Unlock()
	if _, ok := db.tableMap[key]; !ok {
		db.tableMap[key] = &Table{
			key:			key,
			Name:         	tableName,
			ChannelKey:   	ChannelKey,
			ToServerList: 	make([]*ToServer, 0),
			LastToServerID: LastToServerID,
			likeTableList:  make([]*Table,0),
		}
		db.addLikeTable(db.tableMap[key],schemaName,tableName)
		log.Println("AddTable",db.Name,schemaName,tableName,db.channelMap[ChannelKey].Name)
		count.SetTable(db.Name,key)
	} else {
		//log.Println("AddTable key:",key,"db.tableMap[key]：",db.tableMap[key])
		//db.tableMap[key].ChannelKey = ChannelKey
	}
	return true
}

func (db *db) addLikeTable(t *Table ,schemaName,tableName string) {
	if tableName == "*" || strings.Index(tableName, "*") == -1 {
		return
	}
	key := GetSchemaAndTableJoin(schemaName,tableName)
	reqTableName := db.TransferLikeTableReq(tableName)
	reqTagAll,err := regexp.Compile(reqTableName)
	if err != nil{
		log.Println(db.Name," addLikeTable :",key,"reqTableName:",reqTableName," reqTagAll err:",err)
		return
	}
	for k,v := range db.tableMap {
		if strings.Index(k, "*") >= 0 {
			continue
		}
		schemaName0,TableName0 := GetSchemaAndTableBySplit(k)
		if schemaName0 != schemaName{
			continue
		}
		// 假如匹配的表
		if reqTagAll.FindString(TableName0) != "" {
			v.likeTableList = append(v.likeTableList,t)
		}
	}
}


func (db *db) GetTable(schemaName string, tableName string) *Table {
	key := GetSchemaAndTableJoin(schemaName,tableName)
	return db.GetTableByKey(key)
}

func (db *db) GetTableByKey(key string) *Table {
	db.RLock()
	if _, ok := db.tableMap[key]; !ok {
		db.RUnlock()
		//这里判断 > 0 ，假如 == 0 说明是所有表了了，如果是 == * 的情况下，是有 所有表的逻辑，已经存到map中了
		db.Lock()
		defer db.Unlock()
		schemaName,TableName := GetSchemaAndTableBySplit(key)
		key0 := GetSchemaAndTableJoin(schemaName,"*")
		for k,v := range db.tableMap {
			if k == key0 {
				continue
			}
			//库名是 * 或者 table 里没有 * 的，都不匹配
			if strings.Index(k, "*") == -1 {
				continue
			}
			if v.regexpErr {
				continue
			}
			schemaName0,TableName0 := GetSchemaAndTableBySplit(k)
			if schemaName0 != schemaName {
				continue
			}
			reqTagAll,err := regexp.Compile(db.TransferLikeTableReq(TableName0))
			if err != nil{
				v.regexpErr = true
				log.Println(db.Name," GetTable :",k,"TransferLikeTableReq:",db.TransferLikeTableReq(TableName0), "reqTagAll err:",err)
				continue
			}
			if reqTagAll.FindString(TableName) != "" {
				if _,ok := db.tableMap[key];!ok {
					db.tableMap[key] = &Table{
						key:			key,
						ChannelKey:   	v.ChannelKey,
						ToServerList: 	make([]*ToServer, 0),
						likeTableList:	make([]*Table, 0),
					}
					count.SetTable(db.Name,key)
				}
				db.tableMap[key].likeTableList = append(db.tableMap[key].likeTableList,v)
			}
		}
		if _,ok := db.tableMap[key];ok {
			return db.tableMap[key]
		}
		return  nil
	} else {
		defer db.RUnlock()
		return db.tableMap[key]
	}
}

func (db *db) GetTableSelf(schemaName string, tableName string) *Table {
	return db.GetTable(schemaName,tableName)
}

func (db *db) GetTables() map[string]*Table {
	return db.tableMap
}

func (db *db) GetTableByChannelKey(schemaName string, ChanneKey int) (TableMap map[string]*Table) {
	TableMap = make(map[string]*Table,0)
	for k,v := range db.tableMap{
		if v.ChannelKey == ChanneKey && len(v.ToServerList) > 0 {
			TableMap[k] = v
		}
	}
	return
}

/**
删除表和通道的绑定关系
假如存在表和同步关系，则需要将这个表从 binlog 解析中也去删除掉
*/
func (db *db) DelTable(schemaName string, tableName string) bool {
	key := GetSchemaAndTableJoin(schemaName,tableName)
	db.Lock()
	defer db.Unlock()

	if _, ok := db.tableMap[key]; !ok {
		return true
	}

	t := db.tableMap[key]
	toServerLen := len(t.ToServerList)
	for _,toServerInfo := range t.ToServerList {
		if toServerInfo.Status == "running"{
			toServerInfo.Status = "deling"
		}
	}
	delete(db.tableMap,key)
	if tableName != "*" && strings.Index(tableName,"*") >= 0 {
		for _,v := range db.tableMap{
			for index,v0 := range v.likeTableList {
				if  v0 == t {
					if index == len(v.likeTableList) - 1{
						v.likeTableList = v.likeTableList[:len(v.likeTableList)-1]
					}else{
						v.likeTableList = append(v.likeTableList[:index], v.likeTableList[index+1:]...)
					}
					break
				}
			}
		}
	}
	count.DelTable(db.Name,key)
	log.Println("DelTable",db.Name,schemaName,tableName)
	if db.binlogDump != nil && toServerLen > 0 {
		db.DelReplicateDoDb(schemaName,tableName,false)
	}
	return true
}

func (db *db) AddChannel(Name string,MaxThreadNum int) (*Channel,int) {
	db.Lock()
	db.LastChannelID++
	ChannelID := db.LastChannelID
	if _, ok := db.channelMap[ChannelID]; ok {
		db.Unlock()
		return db.channelMap[ChannelID],ChannelID
	}
	c := NewChannel(MaxThreadNum,Name, db)
	db.channelMap[ChannelID] = c
	ch := count.SetChannel(db.Name,Name)
	db.channelMap[ChannelID].SetFlowCountChan(ch)
	db.Unlock()
	log.Println("AddChannel",db.Name,Name,"MaxThreadNum:",MaxThreadNum)
	return db.channelMap[ChannelID],ChannelID
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

type Table struct {
	sync.Mutex
	key				string			// schema+table 组成的key
	Name         	string
	ChannelKey   	int
	LastToServerID  int
	ToServerList 	[]*ToServer
	likeTableList	[]*Table  		// 关联了哪些 模糊匹配的配置
	regexpErr	    bool
}

