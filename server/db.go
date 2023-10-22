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
	"context"
	"fmt"
	"log"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/brokercap/Bifrost/Bristol/mysql"
	inputDriver "github.com/brokercap/Bifrost/input/driver"
	"github.com/brokercap/Bifrost/server/count"
	"github.com/brokercap/Bifrost/server/warning"
)

var dbAndTableSplitChars = "_-"

func GetSchemaAndTableJoin(schema, tableName string) string {
	return schema + dbAndTableSplitChars + tableName
}

func GetSchemaAndTableBySplit(schemaAndTableName string) (schemaName, tableName string) {
	var i int
	// 这里这么操作 是因为 最开始设计 的时候是用  - 分割，现在发现 有不少用户 库名也有 -
	// 为了兼容 ， 这里先判断一下 -, 是否存在，假如哪个用户 库名和表名都有 - 这个时候就会有问题了，但愿没这样的用户嘿嘿
	i = strings.Index(schemaAndTableName, dbAndTableSplitChars)
	if i == -1 {
		if strings.Count(schemaAndTableName, "-") > 1 {
			i = strings.LastIndexAny(schemaAndTableName, "-")
		} else {
			i = strings.IndexAny(schemaAndTableName, "-")
		}
		schemaName = schemaAndTableName[0:i]
		tableName = schemaAndTableName[i+1:]
	} else {
		schemaName = schemaAndTableName[0:i]
		tableName = schemaAndTableName[i+2:]
	}
	return
}

var AllSchemaAndTablekey string = GetSchemaAndTableJoin("*", "*")

var DbLock sync.Mutex

var DbList map[string]*db

func init() {
	DbList = make(map[string]*db, 0)
}

func GetDB(Name string) *db {
	DbLock.Lock()
	defer DbLock.Unlock()
	return DbList[Name]
}

func AddNewDB(Name string, InputType string, inputInfo inputDriver.InputInfo, AddTime int64) *db {
	var r bool = false
	DbLock.Lock()
	if _, ok := DbList[Name]; !ok {
		DbList[Name] = NewDb(Name, InputType, inputInfo, AddTime)
		r = true
	}
	count.SetDB(Name)
	DbLock.Unlock()
	log.Println("Add db Info:", InputType, Name, inputInfo)
	if r == true {
		return DbList[Name]
	} else {
		return nil
	}
}

func UpdateDB(Name string, InputType string, inputInfo inputDriver.InputInfo, UpdateTime int64, updateToServer int8) error {
	DbLock.Lock()
	defer DbLock.Unlock()
	if _, ok := DbList[Name]; !ok {
		return fmt.Errorf(Name + " not exsit")
	}

	if inputInfo.ServerId == 0 {
		return fmt.Errorf("serverId can't be 0")
	}
	dbObj := DbList[Name]
	dbObj.Lock()
	defer dbObj.Unlock()
	if dbObj.ConnStatus != CLOSED {
		return fmt.Errorf("db status must be close")
	}
	dbObj.ConnectUri = inputInfo.ConnectUri
	dbObj.binlogDumpFileName = inputInfo.BinlogFileName
	dbObj.binlogDumpPosition = inputInfo.BinlogPostion
	dbObj.serverId = inputInfo.ServerId
	dbObj.maxBinlogDumpFileName = inputInfo.MaxFileName
	dbObj.maxBinlogDumpPosition = inputInfo.MaxPosition
	dbObj.AddTime = UpdateTime
	if inputInfo.GTID == "" {
		dbObj.gtid = inputInfo.GTID
		dbObj.isGtid = false
	} else {
		dbObj.gtid = inputInfo.GTID
		dbObj.isGtid = true
	}
	log.Println("Update db Info:", InputType, Name, inputInfo)
	if updateToServer == 0 {
		return nil
	}
	var BinlogFileNum int
	if inputInfo.BinlogFileName != "" {
		index := strings.Index(inputInfo.BinlogFileName, ".")
		BinlogFileNum, _ = strconv.Atoi(inputInfo.BinlogFileName[index+1:])
	}

	for key, t := range dbObj.tableMap {
		for _, toServer := range t.ToServerList {
			log.Println("UpdateToServerBinlogPosition:", key, " QueueMsgCount:", toServer.QueueMsgCount, " old:", toServer.BinlogFileNum, toServer.BinlogPosition, " new:", BinlogFileNum, inputInfo.BinlogPostion)
			toServer.UpdateBinlogPosition(BinlogFileNum, inputInfo.BinlogPostion, inputInfo.GTID, 0)
		}
	}
	return nil
}

func GetDBObj(Name string) *db {
	if _, ok := DbList[Name]; !ok {
		return nil
	}
	return DbList[Name]
}

func DelDB(Name string) bool {
	DbLock.Lock()
	defer DbLock.Unlock()
	DBPositionBinlogKey := getDBBinlogkey(DbList[Name])
	if _, ok := DbList[Name]; ok {
		if DbList[Name].ConnStatus == CLOSED {
			for _, c := range DbList[Name].channelMap {
				count.DelChannel(Name, c.Name)
			}
			delete(DbList, Name)
			count.DelDB(Name)
			log.Println("delete db:", Name)
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
	Name                    string            `json:"Name"`
	ConnectUri              string            `json:"ConnectUri"`
	ConnStatus              StatusFlag        `json:"ConnStatus"`
	ConnErr                 string            `json:"ConnErr"`
	channelMap              map[int]*Channel  `json:"ChannelMap"`
	LastChannelID           int               `json:"LastChannelID"`
	tableMap                map[string]*Table `json:"TableMap"`
	isGtid                  bool              `json:"IsGtid"`
	gtid                    string            `json:"Gtid"`
	binlogDumpFileName      string            `json:"BinlogDumpFileName"`
	binlogDumpPosition      uint32            `json:"BinlogDumpPosition"`
	binlogDumpTimestamp     uint32            `json:"BinlogDumpTimestamp"`
	lastEventID             uint64            `json:"LastEventID"`
	replicateDoDb           map[string]uint8  `json:"ReplicateDoDb"`
	serverId                uint32            `json:"ServerId"`
	killStatus              int
	maxBinlogDumpFileName   string `json:"MaxBinlogDumpFileName"`
	maxBinlogDumpPosition   uint32 `json:"MaxBinlogDumpPosition"`
	AddTime                 int64
	DBBinlogKey             []byte                     `json:"-"` // 保存 binlog到levelDB 的key
	lastTransactionTableMap map[string]map[string]bool `json:"-"` // 最近一个事务里更新了数据表
	InputType               string                     `json:"Name"`
	inputDriverObj          inputDriver.Driver         `json:"-"` // 数据源实例化对象
	inputStatusChan         chan *inputDriver.PluginStatus

	statusCtx struct {
		ctx       context.Context
		cancelFun context.CancelFunc
	} `json:"-"`
}

type DbListStruct struct {
	Name                  string
	InputType             string
	ConnectUri            string
	ConnStatus            StatusFlag //close,stop,starting,running
	ConnErr               string
	ChannelCount          int
	LastChannelID         int
	TableCount            int
	BinlogDumpFileName    string
	BinlogDumpPosition    uint32
	IsGtid                bool
	Gtid                  string
	BinlogDumpTimestamp   uint32
	LastEventID           uint64
	MaxBinlogDumpFileName string
	MaxBinlogDumpPosition uint32
	ReplicateDoDb         map[string]uint8
	ServerId              uint32
	AddTime               int64
}

func GetListDb() map[string]DbListStruct {
	var dbListMap map[string]DbListStruct
	dbListMap = make(map[string]DbListStruct, 0)
	DbLock.Lock()
	defer DbLock.Unlock()
	for k, v := range DbList {
		dbListMap[k] = DbListStruct{
			Name:                  v.Name,
			InputType:             v.InputType,
			ConnectUri:            v.ConnectUri,
			ConnStatus:            v.ConnStatus,
			ConnErr:               v.ConnErr,
			ChannelCount:          len(v.channelMap),
			LastChannelID:         v.LastChannelID,
			TableCount:            len(v.tableMap),
			BinlogDumpFileName:    v.binlogDumpFileName,
			BinlogDumpPosition:    v.binlogDumpPosition,
			IsGtid:                v.isGtid,
			Gtid:                  v.gtid,
			LastEventID:           v.lastEventID,
			BinlogDumpTimestamp:   v.binlogDumpTimestamp,
			MaxBinlogDumpFileName: v.maxBinlogDumpFileName,
			MaxBinlogDumpPosition: v.maxBinlogDumpPosition,
			ReplicateDoDb:         v.replicateDoDb,
			ServerId:              v.serverId,
			AddTime:               v.AddTime,
		}
	}
	return dbListMap
}

func GetDbInfo(dbname string) *DbListStruct {
	DbLock.Lock()
	defer DbLock.Unlock()
	v := DbList[dbname]
	if v == nil {
		return &DbListStruct{}
	}
	return &DbListStruct{
		Name:                  v.Name,
		InputType:             v.InputType,
		ConnectUri:            v.ConnectUri,
		ConnStatus:            v.ConnStatus,
		ConnErr:               v.ConnErr,
		ChannelCount:          len(v.channelMap),
		LastChannelID:         v.LastChannelID,
		TableCount:            len(v.tableMap),
		BinlogDumpFileName:    v.binlogDumpFileName,
		BinlogDumpPosition:    v.binlogDumpPosition,
		BinlogDumpTimestamp:   v.binlogDumpTimestamp,
		Gtid:                  v.gtid,
		LastEventID:           v.lastEventID,
		MaxBinlogDumpFileName: v.maxBinlogDumpFileName,
		MaxBinlogDumpPosition: v.maxBinlogDumpPosition,
		ReplicateDoDb:         v.replicateDoDb,
		ServerId:              v.serverId,
		AddTime:               v.AddTime,
	}
}

func NewDbByNull() *db {
	return &db{}
}

func NewDb(Name string, InputType string, inputInfo inputDriver.InputInfo, AddTime int64) *db {
	var isGtid bool
	if inputInfo.GTID != "" {
		isGtid = true
	}
	return &db{
		Name:                    Name,
		ConnectUri:              inputInfo.ConnectUri,
		ConnStatus:              CLOSED,
		ConnErr:                 "",
		LastChannelID:           0,
		channelMap:              make(map[int]*Channel, 0),
		tableMap:                make(map[string]*Table, 0),
		isGtid:                  isGtid,
		gtid:                    inputInfo.GTID,
		binlogDumpFileName:      inputInfo.BinlogFileName,
		binlogDumpPosition:      inputInfo.BinlogPostion,
		maxBinlogDumpFileName:   inputInfo.MaxFileName,
		maxBinlogDumpPosition:   inputInfo.MaxPosition,
		replicateDoDb:           make(map[string]uint8, 0),
		serverId:                inputInfo.ServerId,
		killStatus:              0,
		AddTime:                 AddTime,
		lastTransactionTableMap: make(map[string]map[string]bool, 0),
		inputDriverObj:          nil,
		InputType:               InputType,
	}
}

func (db *db) SetServerId(serverId uint32) {
	db.serverId = serverId
}

func (db *db) SetReplicateDoDb(dbArr []string) bool {
	if db.ConnStatus == CLOSED || db.ConnStatus == STOPPED {
		for i := 0; i < len(dbArr); i++ {
			db.replicateDoDb[dbArr[i]] = 1
		}
		return true
	}
	return false
}

func (db *db) AddReplicateDoDb(schemaName, tableName string, doLock bool) bool {
	if doLock {
		db.Lock()
		defer db.Unlock()
	}
	if tableName == "" {
		return false
	}
	TransferLikeTableReqName := db.TransferLikeTableReq(tableName)
	if db.inputDriverObj != nil {
		db.inputDriverObj.AddReplicateDoDb(schemaName, TransferLikeTableReqName)
		log.Printf("AddReplicateDoDb dbName:%s ,schemaName:%s, tableName:%s , TransferLikeTableReq:%s ", db.Name, schemaName, tableName, TransferLikeTableReqName)
	}
	if _, ok := db.replicateDoDb[schemaName]; !ok {
		db.replicateDoDb[schemaName] = 1
	}
	return true
}

func (db *db) DelReplicateDoDb(schemaName, tableName string, doLock bool) bool {
	if doLock {
		db.Lock()
		defer db.Unlock()
	}
	if tableName == "" {
		return false
	}
	TransferLikeTableReqName := db.TransferLikeTableReq(tableName)
	if db.inputDriverObj != nil {
		db.inputDriverObj.DelReplicateDoDb(schemaName, TransferLikeTableReqName)
		log.Printf("DelReplicateDoDb dbName:%s ,schemaName:%s, tableName:%s , TransferLikeTableReq:%s ", db.Name, schemaName, tableName, TransferLikeTableReqName)

	}
	return true
}

func (db *db) TransferLikeTableReq(tableName string) string {
	if tableName == "" {
		return ""
	}
	var reqTableName string = tableName
	if tableName != "*" && strings.Index(tableName, "*") > -1 {
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
			reqTableName = reqTableName[0:reqTablelen-1] + "(.*)"
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
		if err := recover(); err != nil {
			log.Println(db.Name, " getRightBinlogPosition recover err:", err, " binlogDumpFileName:", db.binlogDumpFileName, " binlogDumpPosition:", db.binlogDumpPosition)
			log.Println(string(debug.Stack()))
			newPosition = 0
		}
	}()
	err := mysql.CheckBinlogIsRight(db.ConnectUri, db.binlogDumpFileName, db.binlogDumpPosition)
	if err == nil {
		return db.binlogDumpPosition
	}
	log.Println(db.Name, " getRightBinlogPosition err:", err, " binlogDumpFileName:", db.binlogDumpFileName, " binlogDumpPosition:", db.binlogDumpPosition)
	if strings.Index(err.Error(), "connect: operation timed out") != -1 {
		return newPosition
	}
	newPosition = mysql.GetNearestRightBinlog(db.ConnectUri, db.binlogDumpFileName, db.binlogDumpPosition, db.serverId, db.getReplicateDoDbMap(), nil)
	return newPosition
}

func (db *db) getReplicateDoDbMap() map[string]map[string]uint8 {
	replicateDoDb := make(map[string]map[string]uint8, 0)
	for k, _ := range db.tableMap {
		schemaName, TableName := GetSchemaAndTableBySplit(k)
		if _, ok := replicateDoDb[schemaName]; !ok {
			replicateDoDb[schemaName] = make(map[string]uint8, 0)
		}
		replicateDoDb[schemaName][TableName] = 1
	}
	return replicateDoDb
}

func (db *db) Start() (b bool) {
	db.statusCtx.ctx, db.statusCtx.cancelFun = context.WithCancel(context.Background())
	db.Lock()
	if db.ConnStatus != CLOSED && db.ConnStatus != STOPPED {
		db.Unlock()
		return false
	}
	db.Unlock()
	b = false
	if db.maxBinlogDumpFileName == db.binlogDumpFileName && db.binlogDumpPosition >= db.maxBinlogDumpPosition {
		return
	}
	if len(db.tableMap) == 0 {
		return
	}
	switch db.ConnStatus {
	case CLOSED:
		db.ConnStatus = STARTING
		// 这里加一个加一个方法里再去执行初始化input,是为防止初始化有空指针等异常导致，不释放锁
		// 不放到 InitInputDriver 中去加锁，因为 GetCurrentPosition 中也会去获取Input，不存在的情况下，也会去初始化一次
		func() {
			db.Lock()
			defer db.Unlock()
			db.InitInputDriver()
		}()
		go db.inputDriverObj.Start(db.inputStatusChan)
		go db.monitorDump()
		break
	case STOPPED:
		db.ConnStatus = RUNNING
		log.Println(db.Name+" monitor:", "running")
		db.inputDriverObj.Start(db.inputStatusChan)
		break
	default:
		return
	}
	go db.CronCalcMinPosition()
	return true
}

/*
InitInputDriver 不进行加锁，是把加锁动作放到外层去做，因为Start方法每次启动都需要强制启动一个新的
并且其GetCurrentPosition也有可能会需要初始化操作
*/

func (db *db) InitInputDriver() {
	inputInfo := inputDriver.InputInfo{
		DbName:         db.Name,
		ConnectUri:     db.ConnectUri,
		GTID:           db.gtid,
		BinlogFileName: db.binlogDumpFileName,
		BinlogPostion:  db.binlogDumpPosition,
		IsGTID:         db.isGtid,

		ServerId:    db.serverId,
		MaxFileName: db.maxBinlogDumpFileName,
		MaxPosition: db.maxBinlogDumpPosition,
	}
	if !db.isGtid {
		inputInfo.GTID = ""
	}
	db.inputStatusChan = make(chan *inputDriver.PluginStatus, 10)
	db.inputDriverObj = inputDriver.Open(db.InputType, inputInfo)
	db.inputDriverObj.SetCallback(db.Callback)
	for key, _ := range db.tableMap {
		schemaName, TableName := GetSchemaAndTableBySplit(key)
		db.AddReplicateDoDb(schemaName, TableName, false)
	}
	db.inputDriverObj.SetEventID(db.lastEventID)

}

func (db *db) Stop() bool {
	db.statusCtx.cancelFun()
	db.Lock()
	defer db.Unlock()
	if db.ConnStatus == RUNNING {
		db.inputDriverObj.Stop()
		db.ConnStatus = STOPPED
	}
	return true
}

func (db *db) Close() bool {
	db.Lock()
	defer db.Unlock()
	if db.ConnStatus != STOPPED && db.ConnStatus != STARTING {
		return true
	}
	db.ConnStatus = CLOSING
	db.inputDriverObj.Close()
	return true
}

func (db *db) monitorDump() (r bool) {
	var lastStatus StatusFlag
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	var i uint8 = 0
	for {
		select {
		case inputStatusInfo := <-db.inputStatusChan:
			if inputStatusInfo == nil {
				break
			}
			timer.Reset(3 * time.Second)
			switch inputStatusInfo.Status {
			case inputDriver.RUNNING:
				i = 0
				db.ConnStatus = RUNNING
				warning.AppendWarning(warning.WarningContent{
					Type:   warning.WARNINGNORMAL,
					DbName: db.Name,
					Body:   " running; last status:" + lastStatus,
				})
			case inputDriver.STARTING:
				db.ConnStatus = STARTING
			case inputDriver.STOPPING:
				db.ConnStatus = STOPPING
			case inputDriver.CLOSING:
				db.ConnStatus = CLOSING
			case inputDriver.STOPPED:
				i = 0
				db.ConnStatus = STOPPED
			case inputDriver.CLOSED:
				db.ConnStatus = CLOSED
				warning.AppendWarning(warning.WarningContent{
					Type:   warning.WARNINGERROR,
					DbName: db.Name,
					Body:   " closed",
				})
			default:
				db.ConnStatus = DEFAULT
			}
			if inputStatusInfo.Error == nil {
				db.ConnErr = ""
			} else {
				db.ConnErr = inputStatusInfo.Error.Error()
			}
			i++
			if i%3 == 0 || strings.Index(db.ConnErr, "parseEvent err") != -1 {
				i = 0
				warning.AppendWarning(warning.WarningContent{
					Type:   warning.WARNINGERROR,
					DbName: db.Name,
					Body:   fmt.Sprintf("err:%s; last status:%s", inputStatusInfo.Error, lastStatus),
				})
			}

			log.Println(db.Name+" monitor:", db.ConnStatus, db.ConnErr)
			lastStatus = db.ConnStatus

			break
		case <-timer.C:
			timer.Reset(3 * time.Second)
			db.saveBinlog()
			break
		}
	}
	return true
}

func (db *db) saveBinlog() {
	p := db.inputDriverObj.GetLastPosition()
	if p == nil {
		return
	}
	//保存位点,这个位点在重启 配置文件恢复的时候
	db.Lock()
	db.binlogDumpFileName, db.binlogDumpPosition, db.binlogDumpTimestamp, db.gtid, db.lastEventID = p.BinlogFileName, p.BinlogPostion, p.Timestamp, p.GTID, p.EventID
	db.Unlock()
	if db.DBBinlogKey == nil {
		db.DBBinlogKey = getDBBinlogkey(db)
	}

	var BinlogFileNum int
	if p.BinlogFileName != "" {
		index := strings.IndexAny(p.BinlogFileName, ".")
		BinlogFileNum, _ = strconv.Atoi(p.BinlogFileName[index+1:])
	}

	var lastParseBinlog = &PositionStruct{
		BinlogFileNum:  BinlogFileNum,
		BinlogPosition: p.BinlogPostion,
		GTID:           p.GTID,
		Timestamp:      p.Timestamp,
		EventID:        p.EventID,
	}
	saveBinlogPosition(db.DBBinlogKey, lastParseBinlog)
}

func (db *db) IgnoreTableToMap(IgnoreTable string) map[string]bool {
	if IgnoreTable == "" {
		return nil
	}
	m := make(map[string]bool, 0)
	for _, tableName := range strings.Split(IgnoreTable, ",") {
		if tableName == "" {
			continue
		}
		m[tableName] = true
	}
	return m
}

func (db *db) AddTable(schemaName string, tableName string, IgnoreTable string, DoTable string, ChannelKey int, LastToServerID int) bool {
	key := GetSchemaAndTableJoin(schemaName, tableName)
	db.Lock()
	defer db.Unlock()
	if _, ok := db.tableMap[key]; !ok {
		db.tableMap[key] = &Table{
			key:            key,
			Name:           tableName,
			ChannelKey:     ChannelKey,
			ToServerList:   make([]*ToServer, 0),
			LastToServerID: LastToServerID,
			likeTableList:  make([]*Table, 0),
			DoTable:        DoTable,
			doTableMap:     db.IgnoreTableToMap(DoTable),
			IgnoreTable:    IgnoreTable,
			ignoreTableMap: db.IgnoreTableToMap(IgnoreTable),
		}
		db.addLikeTable(db.tableMap[key], schemaName, tableName)
		log.Println("AddTable", db.Name, schemaName, tableName, db.channelMap[ChannelKey].Name, " IgnoreTable:", IgnoreTable, "DoTable:", DoTable)
		count.SetTable(db.Name, key)
	}
	return true
}

// 修改 模糊匹配的表规则 需要过滤哪些表不进行匹配
func (db *db) UpdateTable(schemaName string, tableName string, IgnoreTable string, DoTable string) bool {
	key := GetSchemaAndTableJoin(schemaName, tableName)
	db.Lock()
	defer db.Unlock()
	if _, ok := db.tableMap[key]; !ok {
		log.Println("UpdateTable ", db.Name, schemaName, tableName, " not exsit ")
		return false
	}
	db.tableMap[key].DoTable = DoTable
	db.tableMap[key].doTableMap = db.IgnoreTableToMap(DoTable)
	db.tableMap[key].IgnoreTable = IgnoreTable
	db.tableMap[key].ignoreTableMap = db.IgnoreTableToMap(IgnoreTable)
	log.Println("UpdateTable", db.Name, schemaName, tableName, "IgnoreTable:", IgnoreTable, "DoTable:", DoTable)
	return true
}

func (db *db) addLikeTable(t *Table, schemaName, tableName string) {
	if tableName == "*" || strings.Index(tableName, "*") == -1 {
		return
	}
	key := GetSchemaAndTableJoin(schemaName, tableName)
	reqTableName := db.TransferLikeTableReq(tableName)
	reqTagAll, err := regexp.Compile(reqTableName)
	if err != nil {
		log.Println(db.Name, " addLikeTable :", key, "reqTableName:", reqTableName, " reqTagAll err:", err)
		return
	}
	for k, v := range db.tableMap {
		if strings.Index(k, "*") >= 0 {
			continue
		}
		schemaName0, TableName0 := GetSchemaAndTableBySplit(k)
		if schemaName0 != schemaName {
			continue
		}
		// 假如匹配的表
		if reqTagAll.FindString(TableName0) != "" {
			v.likeTableList = append(v.likeTableList, t)
		}
	}
}

func (db *db) GetTable(schemaName string, tableName string) *Table {
	key := GetSchemaAndTableJoin(schemaName, tableName)
	return db.GetTableByKey(key)
}

func (db *db) GetTableByKey(key string) *Table {
	db.RLock()
	if _, ok := db.tableMap[key]; !ok {
		db.RUnlock()
		//这里判断 > 0 ，假如 == 0 说明是所有表了了，如果是 == * 的情况下，是有 所有表的逻辑，已经存到map中了
		db.Lock()
		defer db.Unlock()
		schemaName, TableName := GetSchemaAndTableBySplit(key)
		key0 := GetSchemaAndTableJoin(schemaName, "*")
		for k, v := range db.tableMap {
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
			schemaName0, TableName0 := GetSchemaAndTableBySplit(k)
			if schemaName0 != schemaName {
				continue
			}
			reqTagAll, err := regexp.Compile(db.TransferLikeTableReq(TableName0))
			if err != nil {
				v.regexpErr = true
				log.Println(db.Name, " GetTable :", k, "TransferLikeTableReq:", db.TransferLikeTableReq(TableName0), "reqTagAll err:", err)
				continue
			}
			if reqTagAll.FindString(TableName) != "" {
				if _, ok := db.tableMap[key]; !ok {
					db.tableMap[key] = &Table{
						key:           key,
						ChannelKey:    v.ChannelKey,
						ToServerList:  make([]*ToServer, 0),
						likeTableList: make([]*Table, 0),
					}
					count.SetTable(db.Name, key)
				}
				db.tableMap[key].likeTableList = append(db.tableMap[key].likeTableList, v)
			}
		}
		if _, ok := db.tableMap[key]; ok {
			return db.tableMap[key]
		}
		return nil
	} else {
		defer db.RUnlock()
		return db.tableMap[key]
	}
}

func (db *db) GetTableSelf(schemaName string, tableName string) *Table {
	return db.GetTable(schemaName, tableName)
}

func (db *db) GetTables() map[string]*Table {
	return db.tableMap
}

func (db *db) GetTableByChannelKey(schemaName string, ChanneKey int) (TableMap map[string]*Table) {
	TableMap = make(map[string]*Table, 0)
	for k, v := range db.tableMap {
		if v.ChannelKey == ChanneKey && len(v.ToServerList) > 0 {
			TableMap[k] = v
		}
	}
	return
}

/*
*
删除表和通道的绑定关系
假如存在表和同步关系，则需要将这个表从 binlog 解析中也去删除掉
*/
func (db *db) DelTable(schemaName string, tableName string) bool {
	key := GetSchemaAndTableJoin(schemaName, tableName)
	db.Lock()
	defer db.Unlock()

	if _, ok := db.tableMap[key]; !ok {
		return true
	}

	t := db.tableMap[key]
	toServerLen := len(t.ToServerList)
	for _, toServerInfo := range t.ToServerList {
		if toServerInfo.Status == RUNNING {
			toServerInfo.Status = DELING
		}
	}
	delete(db.tableMap, key)
	if tableName != "*" && strings.Index(tableName, "*") >= 0 {
		for _, v := range db.tableMap {
			for index, v0 := range v.likeTableList {
				if v0 == t {
					if index == len(v.likeTableList)-1 {
						v.likeTableList = v.likeTableList[:len(v.likeTableList)-1]
					} else {
						v.likeTableList = append(v.likeTableList[:index], v.likeTableList[index+1:]...)
					}
					break
				}
			}
		}
	}
	count.DelTable(db.Name, key)
	log.Println("DelTable", db.Name, schemaName, tableName)
	if db.inputDriverObj != nil && toServerLen > 0 {
		db.DelReplicateDoDb(schemaName, tableName, false)
	}
	return true
}

func (db *db) AddChannel(Name string, MaxThreadNum int) (*Channel, int) {
	db.Lock()
	db.LastChannelID++
	ChannelID := db.LastChannelID
	if _, ok := db.channelMap[ChannelID]; ok {
		db.Unlock()
		return db.channelMap[ChannelID], ChannelID
	}
	c := NewChannel(MaxThreadNum, Name, db)
	db.channelMap[ChannelID] = c
	ch := count.SetChannel(db.Name, Name)
	db.channelMap[ChannelID].SetFlowCountChan(ch)
	db.Unlock()

	log.Println("AddChannel", db.Name, Name, "MaxThreadNum:", MaxThreadNum)
	return db.channelMap[ChannelID], ChannelID
}

func (db *db) ListChannel() map[int]*Channel {
	db.Lock()
	defer db.Unlock()
	return db.channelMap
}

func (db *db) GetChannel(channelID int) *Channel {
	if _, ok := db.channelMap[channelID]; !ok {
		return nil
	}
	return db.channelMap[channelID]
}

/*
获取 input 当前最新位点信息
*/

func (db *db) GetCurrentPosition() (*inputDriver.PluginPosition, error) {
	inputDriverObj := db.GetInputDriverObj()
	if inputDriverObj == nil {
		return nil, nil
	}
	// 这里通过 db.GetInputDriverObj 内部去加锁,并返回一个 inputDriverObj 对象
	// 再对input调用GetCurrentPosition方法,是防止input 内部再加锁的情况下,会被触发锁未被释放,进入死锁的可能
	return inputDriverObj.GetCurrentPosition()
}

func (db *db) GetInputDriverObj() inputDriver.Driver {
	db.Lock()
	defer db.Unlock()
	if db.inputDriverObj == nil {
		db.InitInputDriver()
	}
	return db.inputDriverObj
}
