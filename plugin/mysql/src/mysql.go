package src

import (
	dbDriver "database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"log"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

type TableDataStruct struct {
	Data       []*pluginDriver.PluginDataType
	CommitData []*pluginDriver.PluginDataType // commit 提交的数据列表，Data 每 BatchSize 数据量划分为一个最后提交的commit
}

func init() {
	pluginDriver.Register(OutputName, NewConn, VERSION, BIFROST_VERION)
}

type fieldStruct struct {
	ToField        string
	FromMysqlField string
	ToFieldType    string
	ToFieldDefault *string
}

func NewTableData() *TableDataStruct {
	CommitData := make([]*pluginDriver.PluginDataType, 0)
	CommitData = append(CommitData, nil)
	return &TableDataStruct{
		Data:       make([]*pluginDriver.PluginDataType, 0),
		CommitData: CommitData,
	}
}

type Conn struct {
	uri              *string
	status           string
	p                *PluginParam
	conn             *mysqlDB
	err              error
	serverVersion    string
	isTiDB           bool
	isStarRocks      bool
	starRocksBeCount int
}

type PluginParam struct {
	Field                []fieldStruct
	BatchSize            int
	Schema               string
	Table                string
	NullTransferDefault  bool //是否将null值强制转成相对应类型的默认值
	SyncMode             SyncMode
	BifrostMustBeSuccess bool // bifrost server 保留,数据是否能丢

	schemaAndTable string
	replaceInto    bool // 记录当前表是否有replace into操作
	PriKey         []fieldStruct
	toPriKey       string // toMysql 主键字段
	fromPriKey     string //	对应 from mysql 的主键id
	Data           *TableDataStruct
	fieldCount     int
	tableMap       map[string]*PluginParam0 // 需要自动创建ck表结构 创建之后表基本信息
	toDatabaseMap  map[string]bool          // ck 里,database 列表信息，database name 做为key，用于缓存
	AutoTable      bool                     // 是否自动匹配数据表
	stmtArr        []dbDriver.Stmt
	SkipBinlogData *pluginDriver.PluginDataType // 在执行 skip 的时候 ，进行传入进来的时候需要要过滤的 位点，在每次commit之后，这个数据会被清空
}

type PluginParam0 struct {
	Field          []fieldStruct
	SchemaName     string
	TableName      string
	SchemaAndTable string
	PriKey         []fieldStruct // 主键对应关系
	FromPriKey     string        // 源表的 主键 字段
	ToPriKey       string        // 目标库的 主键 字段
}

func NewConn() pluginDriver.Driver {
	return &Conn{status: "close"}
}

func (This *Conn) SetOption(uri *string, param map[string]interface{}) {
	This.uri = uri
	return
}

func (This *Conn) Open() error {
	This.Connect()
	return nil
}

func (This *Conn) CheckUri() error {
	This.Connect()
	if This.conn.err != nil {
		return This.conn.err
	}
	if This.conn == nil {
		This.Close()
		return fmt.Errorf("connect error")
	}

	var schemaList []string
	func() {
		defer func() {
			return
		}()
		schemaList = This.conn.GetSchemaList()
	}()
	if len(schemaList) == 0 {
		This.Close()
		return fmt.Errorf("schema count is 0 (not in system)")
	}
	return nil
}

func (This *Conn) GetUriExample() string {
	return "root:root@tcp(127.0.0.1:3306)/test"
}

func (This *Conn) initTableInfo() {
	if This.p.AutoTable == false {
		This.initToMysqlTableFieldType()
	} else {
		This.p.tableMap = make(map[string]*PluginParam0, 0)
		This.initToDatabaseMap()
	}
}

func (This *Conn) GetParam(p interface{}) (*PluginParam, error) {
	s, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	var param PluginParam
	err2 := json.Unmarshal(s, &param)
	if err2 != nil {
		return nil, err2
	}
	if param.BatchSize == 0 {
		param.BatchSize = 500
	}
	if param.Table == "" {
		param.AutoTable = true
	}
	param.Data = NewTableData()
	if param.AutoTable == false {
		param.schemaAndTable = "`" + param.Schema + "`.`" + param.Table + "`"
		param.toPriKey = param.PriKey[0].ToField
		param.fromPriKey = param.PriKey[0].FromMysqlField
	}
	param.stmtArr = make([]dbDriver.Stmt, 4)
	if param.SyncMode == "" {
		param.SyncMode = SYNCMODE_NORMAL
	}

	This.p = &param
	This.initTableInfo()
	This.initVersion()
	if !This.isTiDB {
		// 假如是TiDB,则说明肯定不是starrocks
		This.initIsStarrock()
	}
	return This.p, nil
}

func (This *Conn) SetParam(p interface{}) (interface{}, error) {
	if p == nil {
		return nil, fmt.Errorf("param is nil")
	}
	switch p.(type) {
	case *PluginParam:
		This.p = p.(*PluginParam)
		return p, nil
	default:
		return This.GetParam(p)
	}
}

func (This *Conn) initToMysqlTableFieldType() {
	defer func() {
		if err := recover(); err != nil {
			log.Println(string(debug.Stack()))
			This.conn.err = fmt.Errorf(string(debug.Stack()))
		}
	}()
	if This.p == nil {
		return
	}

	fields := This.conn.GetTableFields(This.p.Schema, This.p.Table)
	if This.conn.err != nil {
		This.err = This.conn.err
		return
	}
	if len(fields) == 0 {
		return
	}
	ckFieldsMap := make(map[string]TableStruct)
	for _, v := range fields {
		ckFieldsMap[v.COLUMN_NAME] = v
	}
	list := make([]fieldStruct, 0)
	for k, v := range This.p.Field {
		This.p.Field[k].ToFieldType = ckFieldsMap[v.ToField].DATA_TYPE
		if strings.ToLower(ckFieldsMap[v.ToField].EXTRA) == "auto_increment" {
			This.p.Field[k].ToFieldDefault = nil
			if v.FromMysqlField != "" {
				list = append(list, This.p.Field[k])
			}
		} else {
			// mysql 里的默认值是在 insert 语句执行的时候，sql 里没有指定字段名的情况下，自动填充
			// 假如有默认值 ，但是允许为 null 的时候，假如 sql 里指定值为 null，还是可以将 null 写进去的
			// 但是 bf 同步写数据的时候，源端是可能为 null ，目标表 是 not null default 值
			// 因为后面 tansfer 函数只使用了 default 值，没做是否可以为 null 判断 ，这里进行统一判断 可以为 null 的情况下，默认值为 null
			if strings.ToUpper(ckFieldsMap[v.ToField].IS_NULLABLE) == "YES" {
				This.p.Field[k].ToFieldDefault = nil
			} else {
				This.p.Field[k].ToFieldDefault = ckFieldsMap[v.ToField].COLUMN_DEFAULT
			}
			list = append(list, This.p.Field[k])
		}
	}
	This.p.Field = list
	This.p.fieldCount = len(list)
}

func (This *Conn) GetSchemaName(data *pluginDriver.PluginDataType) (SchemaName string) {
	if This.p.Schema == "" {
		SchemaName = data.SchemaName
	} else {
		SchemaName = This.p.Schema
	}
	return
}

func (This *Conn) GetTableName(data *pluginDriver.PluginDataType) (TableName string) {
	if This.p.Table == "" {
		TableName = data.TableName
	} else {
		TableName = This.p.Table
	}
	return
}

func (This *Conn) GetSchemaAndTable(data *pluginDriver.PluginDataType) (SchemaName, TableName, SchemaAndTable string) {
	SchemaName = This.GetSchemaName(data)
	TableName = This.GetTableName(data)
	SchemaAndTable = fmt.Sprintf("`%s`.`%s`", SchemaName, TableName)
	return
}

func (This *Conn) CreateTableAndGetTableFieldsType(data *pluginDriver.PluginDataType) (tableFields *PluginParam0, err error) {
	tableFields, _ = This.getAutoTableFieldType(data)
	// 这里无视 是否返回 error, 因为有可能会返回 查不到表的 错误,这里直接跳过这个错误,后面遇到错误会进进行再处理
	/*
		if err != nil {
			return nil, err
		}
	*/
	if tableFields != nil {
		return tableFields, nil
	}
	// 这里无视是否创建成功,如果失败了,后面的建表逻辑,也肯定报错

	_ = This.conn.CreateDatabase(This.GetSchemaName(data))
	createTableSql, isContinue := This.TransferToCreateTableSql(data)
	if createTableSql == "" {
		if isContinue {
			return nil, nil
		} else {
			log.Printf("[ERROR] output[%s] get create table sql is empty,data:%+v \n", OutputName, data)
			return nil, errors.New("get create table sql is empty")
		}
	}

	err = This.conn.Exec(createTableSql)
	if err != nil {
		return nil, err
	}
	tableFields, err = This.getAutoTableFieldType(data)
	return
}

func (This *Conn) getAutoTableFieldType(data *pluginDriver.PluginDataType) (*PluginParam0, error) {
	defer func() {
		if err := recover(); err != nil {
			log.Println(string(debug.Stack()))
			This.conn.err = fmt.Errorf(string(debug.Stack()))
		}
	}()
	var SchemaName, TableName, key = This.GetSchemaAndTable(data)
	if _, ok := This.p.tableMap[key]; ok {
		return This.p.tableMap[key], nil
	}
	fields := This.conn.GetTableFields(SchemaName, TableName)
	if This.conn.err != nil {
		This.err = This.conn.err
		return nil, This.err
	}
	if len(fields) == 0 {
		err := fmt.Errorf("SchemaName:%s, TableName:%s not exsit", SchemaName, data.TableName)
		return nil, err
	}
	fieldList := make([]fieldStruct, 0)
	priKeyList := make([]fieldStruct, 0)
	var fromPriKey, toPriKey string
	var ok bool
	for _, v := range fields {
		var fromFieldName string
		if _, ok = data.Rows[0][v.COLUMN_NAME]; !ok {
			switch v.COLUMN_NAME {
			case "binlog_event_type":
				fromFieldName = "{$EventType}"
				break
			case "binlog_timestamp", "binlogtimestamp":
				fromFieldName = "{$BinlogTimestamp}"
				break
			case "binlogfilenum", "binlog_filenum":
				fromFieldName = "{$BinlogFileNum}"
				break
			case "binlogposition", "binlog_position":
				fromFieldName = "{$BinlogPosition}"
				break
			case "binlog_datetime", "binlogdatetime":
				fromFieldName = "{$BinlogDateTime}"
				break
			default:
				fromFieldName = v.COLUMN_NAME
				break
			}
		} else {
			fromFieldName = v.COLUMN_NAME
		}
		var ToFieldDefault *string
		// mysql 里的默认值是在 insert 语句执行的时候，sql 里没有指定字段名的情况下，自动填充
		// 假如有默认值 ，但是允许为 null 的时候，假如 sql 里指定值为 null，还是可以将 null 写进去的
		// 但是 bf 同步写数据的时候，源端是可能为 null ，目标表 是 not null default 值
		// 因为后面 tansfer 函数只使用了 default 值，没做是否可以为 null 判断 ，这里进行统一判断 可以为 null 的情况下，默认值为 null
		// 同 initToMysqlTableFieldType 函数内部注释
		if strings.ToUpper(v.IS_NULLABLE) == "YES" {
			ToFieldDefault = nil
		} else {
			ToFieldDefault = v.COLUMN_DEFAULT
		}
		field := fieldStruct{ToField: v.COLUMN_NAME, ToFieldType: v.DATA_TYPE, FromMysqlField: fromFieldName, ToFieldDefault: ToFieldDefault}
		if strings.ToUpper(v.COLUMN_KEY) == "PRI" {
			field.ToFieldDefault = nil
			priKeyList = append(priKeyList, field)
			if fromPriKey == "" || v.EXTRA == "auto_increment" {
				fromPriKey = v.COLUMN_NAME
				toPriKey = v.COLUMN_NAME
			}
		}
		// 假如starrocks表字段允许为null
		// 并且同时是BIGINT类型
		// 同时源端数据中又不存在,则认为其为自增字段,
		if v.COLUMN_DEFAULT == nil && strings.Contains(strings.ToUpper(v.DATA_TYPE), "BIGINT") {
			if _, ok = data.Rows[len(data.Rows)-1][v.COLUMN_NAME]; !ok {
				continue
			}
		}
		fieldList = append(fieldList, field)
	}
	if len(fieldList) == 0 {
		return nil, fmt.Errorf("not found %s.%s", SchemaName, data.TableName)
	}
	if fromPriKey == "" && len(data.Pri) > 0 {
		fromPriKey = data.Pri[0]
		toPriKey = data.Pri[0]
	}

	p := &PluginParam0{
		Field:          fieldList,
		PriKey:         priKeyList,
		SchemaName:     SchemaName,
		TableName:      data.TableName,
		SchemaAndTable: key,
		FromPriKey:     fromPriKey,
		ToPriKey:       toPriKey,
	}
	This.p.tableMap[key] = p
	return p, nil
}

// 查出 目标库 里所有database,放到 map 中，用于缓存
func (This *Conn) initToDatabaseMap() {
	This.p.toDatabaseMap = make(map[string]bool, 0)
	defer func() {
		if err := recover(); err != nil {
			return
		}
	}()
	SchemaList := This.conn.GetSchemaList()
	for _, Name := range SchemaList {
		This.p.toDatabaseMap[Name] = true
	}
	return
}

func (This *Conn) initVersion() {
	defer func() {
		if err := recover(); err != nil {
			log.Println("plugin mysql initVersion recover:", err, string(debug.Stack()))
			return
		}
	}()
	if This.conn == nil {
		return
	}
	This.serverVersion = This.conn.SelectVersion()
	if strings.Contains(This.serverVersion, "TiDB") {
		This.isTiDB = true
	}
}

func (This *Conn) Connect() bool {
	This.conn = NewMysqlDBConn(*This.uri)
	if This.conn.err == nil {
		This.conn.conn.Exec("SET NAMES utf8mb4", []dbDriver.Value{})
	}
	return true
}

func (This *Conn) ReConnect() bool {
	defer func() {
		if err := recover(); err != nil {
			This.conn.err = fmt.Errorf(fmt.Sprint(err) + " debug:" + string(debug.Stack()))
			This.err = This.conn.err
		}
	}()
	if This.conn != nil {
		This.closeStmt0()
		This.conn.Close()
	}
	This.Connect()
	if This.conn.err == nil {
		This.initTableInfo()
	}
	return true
}

func (This *Conn) StmtClose() {
	for k, stmt := range This.p.stmtArr {
		if stmt != nil {
			func() {
				defer func() {
					if err := recover(); err != nil {
						This.conn.err = fmt.Errorf("StmtClose err:%s", fmt.Sprint(err))
						return
					}
				}()
				stmt.Close()
			}()
		}
		This.p.stmtArr[k] = nil
	}
}

func (This *Conn) Close() bool {
	if This.conn != nil {
		func() {
			defer func() {
				if err := recover(); err != nil {
					return
				}
			}()
			This.conn.Close()
		}()
	}
	return true
}

func (This *Conn) sendToCacheList(data *pluginDriver.PluginDataType, retry bool) (LastSuccessCommitData *pluginDriver.PluginDataType, ErrData *pluginDriver.PluginDataType, err error) {
	var n int
	if retry == false {
		This.p.Data.Data = append(This.p.Data.Data, data)
	}
	n = len(This.p.Data.Data)
	if This.p.BatchSize <= n {
		LastSuccessCommitData, ErrData, err = This.AutoCommit()
		if LastSuccessCommitData != nil {
			This.p.SkipBinlogData = nil
		}
		return
	}
	return nil, nil, nil
}

func (This *Conn) Insert(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return This.sendToCacheList(data, retry)
}

func (This *Conn) Update(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return This.sendToCacheList(data, retry)
}

func (This *Conn) Del(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return This.sendToCacheList(data, retry)
}

func (This *Conn) Query(data *pluginDriver.PluginDataType, retry bool) (LastSuccessCommitData *pluginDriver.PluginDataType, ErrData *pluginDriver.PluginDataType, err error) {
	if This.p.AutoTable == false || data.Query == "" {
		return nil, nil, nil
	}
	switch data.Query {
	case "COMMIT", "commit", "BEGIN", "begin":
		return nil, nil, nil
	default:
		break
	}
	for {
		LastSuccessCommitData, ErrData, err = This.AutoCommit()
		if err != nil {
			break
		}
		if len(This.p.Data.Data) == 0 {
			if This.CheckDataSkip(data) {
				This.p.SkipBinlogData = nil
				return data, nil, nil
			}
			newSqlArr := This.TranferQuerySql(data)
			if len(newSqlArr) == 0 {
				log.Println("transfer sql error!", data)
				return nil, data, fmt.Errorf("transfer sql error")
			}
			if This.conn.err != nil {
				This.ReConnect()
			}
			if This.conn.err != nil {
				return nil, nil, This.conn.err
			}
			for _, newSql := range newSqlArr {
				if newSql == "" {
					continue
				}
				_, This.conn.err = This.conn.conn.Exec(newSql, []dbDriver.Value{})
				if This.conn.err != nil {
					log.Printf("plugin mysql, exec sql:%s err:%s", newSql, This.conn.err)
					return nil, data, This.conn.err
				}
			}
			break
		}
	}
	return
}

func (This *Conn) Commit(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	n := len(This.p.Data.Data)
	if n == 0 {
		return data, nil, nil
	}
	n0 := n / This.p.BatchSize
	if len(This.p.Data.CommitData)-1 < n0 {
		This.p.Data.CommitData = append(This.p.Data.CommitData, data)
	} else {
		This.p.Data.CommitData[n0] = data
	}
	return nil, nil, nil
}

func (This *Conn) TimeOutCommit() (LastSuccessCommitData *pluginDriver.PluginDataType, ErrData *pluginDriver.PluginDataType, err error) {
	LastSuccessCommitData, ErrData, err = This.AutoCommit()
	if LastSuccessCommitData != nil {
		This.p.SkipBinlogData = nil
	}
	return
}

func (This *Conn) getMySQLData(data *pluginDriver.PluginDataType, index int, key string) interface{} {
	if key == "" {
		return nil
	}
	if _, ok := data.Rows[index][key]; ok {
		return data.Rows[index][key]
	}
	switch key {
	case "{$EventType}":
		return data.EventType
		break
	case "{$Timestamp}":
		return time.Now().Unix()
		break
	case "{$BinlogTimestamp}":
		return data.Timestamp
		break
	case "{$BinlogFileNum}":
		return data.BinlogFileNum
		break
	case "{$BinlogPosition}":
		return data.BinlogPosition
		break
	default:
		return pluginDriver.TransfeResult(key, data, index, true)
		break
	}
	return ""
}

// 设置跳过的位点
func (This *Conn) Skip(SkipData *pluginDriver.PluginDataType) error {
	This.p.SkipBinlogData = SkipData
	return nil
}

func (This *Conn) AutoCommit() (LastSuccessCommitData *pluginDriver.PluginDataType, ErrData *pluginDriver.PluginDataType, e error) {
	defer func() {
		if err := recover(); err != nil {
			e = fmt.Errorf(string(debug.Stack()))
			log.Println(string(debug.Stack()))
			This.conn.err = e
			This.err = e
		}
	}()
	n := len(This.p.Data.Data)
	if n == 0 {
		return nil, nil, nil
	}
	if This.p.SyncMode == SYNCMODE_NO_SYNC_DATA {
		binlogEvent := This.p.Data.CommitData[len(This.p.Data.CommitData)-1]
		This.p.Data = NewTableData()
		return binlogEvent, nil, nil
	}
	if This.conn.err != nil {
		This.ReConnect()
	}
	if This.conn.err != nil {
		return nil, nil, This.conn.err
	}
	if n > This.p.BatchSize {
		n = This.p.BatchSize
	}
	list := This.p.Data.Data[:n]
	if This.p.AutoTable {
		ErrData, e = This.AutoTableCommit(list)
	} else {
		ErrData, e = This.NotAutoTableCommit(list)
	}
	if e != nil {
		log.Println("e:", e)
		return
	}
	var binlogEvent *pluginDriver.PluginDataType
	if len(This.p.Data.Data) <= int(This.p.BatchSize) {
		binlogEvent = This.p.Data.CommitData[0]
		This.p.Data = NewTableData()
	} else {
		This.p.Data.Data = This.p.Data.Data[n:]
		if len(This.p.Data.CommitData) > 0 {
			binlogEvent = This.p.Data.CommitData[0]
			This.p.Data.CommitData = This.p.Data.CommitData[1:]
		}
	}
	return binlogEvent, nil, nil
}

func (This *Conn) NotAutoTableCommit(list []*pluginDriver.PluginDataType) (ErrData *pluginDriver.PluginDataType, e error) {
	This.conn.err = This.conn.Begin()
	if This.conn.err != nil {
		return nil, This.conn.err
	}
	ErrData = This.commitData(list)

	if This.conn.err != nil {
		This.err = This.conn.err
		//log.Println("plugin mysql conn.err",This.err)
		return ErrData, This.err
	}
	if This.err != nil {
		This.conn.err = This.conn.Rollback()
		log.Println("plugin mysql err", This.err)
		return ErrData, This.err
	}
	This.conn.err = This.conn.Commit()
	This.StmtClose()
	if This.conn.err != nil {
		return nil, This.conn.err
	}
	return
}

// 自动创建表的提交
func (This *Conn) AutoTableCommit(list []*pluginDriver.PluginDataType) (ErrData *pluginDriver.PluginDataType, e error) {
	dataMap := make(map[string][]*pluginDriver.PluginDataType, 0)
	var ok bool
	for _, PluginData := range list {
		key := PluginData.SchemaName + "." + PluginData.TableName
		if _, ok = dataMap[key]; !ok {
			dataMap[key] = make([]*pluginDriver.PluginDataType, 0)
		}
		dataMap[key] = append(dataMap[key], PluginData)
	}
	for _, data := range dataMap {
		p, err := This.CreateTableAndGetTableFieldsType(data[0])
		if err != nil {
			return data[0], err
		}
		if p == nil {
			continue
		}
		This.p.Field = p.Field
		This.p.fieldCount = len(p.Field)
		This.p.schemaAndTable = p.SchemaAndTable
		This.p.PriKey = p.PriKey
		This.p.toPriKey = p.ToPriKey
		This.p.fromPriKey = p.FromPriKey
		This.conn.err = This.conn.Begin()
		if This.conn.err != nil {
			This.err = This.conn.err
			break
		}

		ErrData = This.commitData(data)
		if This.conn.err != nil {
			This.err = This.conn.err
		}
		if This.err != nil {
			This.conn.err = This.conn.Rollback()
			log.Printf("[ERROR] output[%s] AutoTableCommit commitData err:%+v \n", OutputName, This.err)
			return ErrData, This.err
		}
		This.conn.err = This.conn.Commit()
		This.StmtClose()
		if This.conn.err != nil {
			break
		}
	}
	return
}

func (This *Conn) commitData(list []*pluginDriver.PluginDataType) (ErrData *pluginDriver.PluginDataType) {
	switch This.p.SyncMode {
	case SYNCMODE_NORMAL:
		if This.IsStarRocks() {
			ErrData = This.StarRocksCommitNormal(list)
		} else {
			ErrData = This.CommitNormal(list)
		}
		break
	case SYNCMODE_LOG_UPDATE:
		if This.IsStarRocks() {
			ErrData = This.StarRocksCommit_Append(list)
		} else {
			ErrData = This.CommitLogMod_Update(list)
		}
		break
	case SYNCMODE_LOG_APPEND:
		if This.IsStarRocks() {
			ErrData = This.StarRocksCommit_Append(list)
		} else {
			ErrData = This.CommitLogMod_Append(list)
		}
		break
	default:
		This.err = fmt.Errorf("同步模式ERROR:%s", This.p.SyncMode)
		break
	}
	return
}

func (This *Conn) dataTypeTransfer(data interface{}, fieldName string, toDataType string, defaultVal *string) (v dbDriver.Value, e error) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("[ERROR] output[%s] dataTypeTransfer pacnic:%+v stack:%+v \n", OutputName, err, string(debug.Stack()))
			e = fmt.Errorf(fieldName + " " + fmt.Sprint(err))
		}
	}()
	if data == nil {
		if This.p.NullTransferDefault == false {
			if defaultVal == nil {
				v = nil
				return
			} else {
				// 这里要判断 是不是 bit 类型，因为 bit 类型在传输值 上 必须 为 int64 类型，不能是字符串
				if toDataType == "bit" {
					v, _ = strconv.ParseInt(*defaultVal, 10, 64)
				} else {
					v = *defaultVal
				}
				return
			}
		} else {
			//假如配置是强制转成默认值
			switch toDataType {
			case "int", "tinyint", "smallint", "mediumint", "bigint", "bool":
				v = "0"
				break
			case "bit":
				v = int64(0)
				break
			case "date":
				v = "1970-01-01"
				break
			case "timestamp":
				v = "1970-01-01 00:00:01"
				break
			case "datetime":
				v = "1000-01-01 00:00:00"
				break
			case "time":
				v = "00:00:01"
				break
			case "year":
				v = "1970"
				break
			case "float", "double", "decimal", "number", "point":
				v = "0.00"
				break
			case "json":
				v = "{}"
				break
			default:
				v = ""
				break
			}
			return
		}
	}
	switch data.(type) {
	case bool:
		if data.(bool) == false {
			data = "0"
		} else {
			data = "1"
		}
		break
	default:
		break
	}
	switch toDataType {
	case "bool":
		switch fmt.Sprint(data) {
		case "0", "":
			v = "0"
			break
		default:
			v = "1"
		}
		break
	case "bit":
		switch data.(type) {
		case string:
			v, _ = strconv.ParseInt(data.(string), 10, 64)
			break
		case int64:
			v = data.(int64)
		case float64:
			v = int64(data.(float64))
		case float32:
			v = int64(data.(float32))
		default:
			v, _ = strconv.ParseInt(fmt.Sprint(data), 10, 64)
			break
		}
		break
	case "set":
		switch data.(type) {
		case []string, []interface{}:
			v = strings.Replace(strings.Trim(fmt.Sprint(data), "[]"), " ", ",", -1)
			break
		default:
			v = fmt.Sprint(data)
			break
		}
		break
	case "json":
		switch reflect.TypeOf(data).Kind() {
		case reflect.Array, reflect.Slice, reflect.Map:
			var c []byte
			c, e = json.Marshal(data)
			if e != nil {
				return
			}
			v = string(c)
			break
		default:
			e = fmt.Errorf("field:%s ,data source type: %s, is not object or array, s ", fieldName, reflect.TypeOf(data).Kind().String())
		}
		break
	default:
		v, e = This.data2String(data)
		if e != nil {
			e = fmt.Errorf("field:%s ,%s", fieldName, e.Error())
		}
		break
	}
	return
}

func (This *Conn) data2String(data interface{}) (v string, e error) {
	switch reflect.TypeOf(data).Kind() {
	case reflect.String:
		switch data.(type) {
		case json.Number:
			return data.(json.Number).String(), nil
		default:
			return fmt.Sprint(data), nil
		}
	case reflect.Array, reflect.Slice, reflect.Map:
		var c []byte
		c, e = json.Marshal(data)
		if e != nil {
			e = fmt.Errorf("data source type: %s , json.Marshal err: %s ", reflect.TypeOf(data).Kind().String(), e.Error())
			return
		}
		v = string(c)
		break
	case reflect.Float32:
		v = strconv.FormatFloat(float64(data.(float32)), 'E', -1, 32)
	case reflect.Float64:
		v = strconv.FormatFloat(data.(float64), 'E', -1, 64)
	default:
		v = fmt.Sprint(data)
	}
	return
}

func (This *Conn) getStmt(Type EventType) dbDriver.Stmt {
	if This.p.stmtArr[Type] != nil {
		return This.p.stmtArr[Type]
	}
	switch Type {
	case REPLACE_INSERT:
		fields := ""
		values := ""
		for _, v := range This.p.Field {
			if fields == "" {
				fields = "`" + v.ToField + "`"
				values = "?"
			} else {
				fields += ",`" + v.ToField + "`"
				values += ",?"
			}
		}
		sql := "REPLACE INTO " + This.p.schemaAndTable + " (" + fields + ") VALUES (" + values + ")"
		This.p.stmtArr[Type], This.conn.err = This.conn.conn.Prepare(sql)
		if This.conn.err != nil {
			log.Println("mysql getStmt REPLACE_INSERT err:", This.conn.err, sql)
		}
		break
	case INSERT:
		fields := ""
		values := ""
		for _, v := range This.p.Field {
			if fields == "" {
				fields = "`" + v.ToField + "`"
				values = "?"
			} else {
				fields += ",`" + v.ToField + "`"
				values += ",?"
			}
		}
		sql := "INSERT INTO " + This.p.schemaAndTable + " (" + fields + ") VALUES (" + values + ")"
		This.p.stmtArr[Type], This.conn.err = This.conn.conn.Prepare(sql)
		if This.conn.err != nil {
			log.Println("mysql getStmt INSERT err:", This.conn.err, sql)
		}
		break
	case DELETE:
		where := ""
		for _, v := range This.p.PriKey {
			if where == "" {
				where = "`" + v.ToField + "`=?"
			} else {
				where += " AND `" + v.ToField + "`=?"
			}
		}
		This.p.stmtArr[Type], This.conn.err = This.conn.conn.Prepare("DELETE FROM " + This.p.schemaAndTable + " WHERE " + where)
		if This.conn.err != nil {
			log.Println("mysql getStmt DELETE err:", This.conn.err)
		}
		break
	case UPDATE:
		fields := ""
		values := ""
		fields2 := ""
		for _, v := range This.p.Field {
			if fields == "" {
				fields = "`" + v.ToField + "`"
				values = "?"
				fields2 = "`" + v.ToField + "`=?"
			} else {
				fields += ",`" + v.ToField + "`"
				values += ",?"
				fields2 += ",`" + v.ToField + "`=?"
			}
		}
		sql := "INSERT INTO " + This.p.schemaAndTable + " (" + fields + ") VALUES (" + values + ") ON DUPLICATE KEY UPDATE " + fields2
		This.p.stmtArr[Type], This.conn.err = This.conn.conn.Prepare(sql)
		if This.conn.err != nil {
			log.Println("mysql getStmt INSERT ON DUPLICATE KEY UPDATE err:", This.conn.err, sql)
		}
		break
	}

	return This.p.stmtArr[Type]
}

func (This *Conn) closeStmt0() {
	for k, _ := range This.p.stmtArr {
		This.p.stmtArr[k] = nil
	}
}

func (This *Conn) CheckDataSkip(data *pluginDriver.PluginDataType) bool {
	if This.p.SkipBinlogData != nil && This.p.SkipBinlogData.BinlogFileNum == data.BinlogFileNum && This.p.SkipBinlogData.BinlogPosition == data.BinlogPosition {
		if This.p.SkipBinlogData.BinlogFileNum == data.BinlogFileNum && This.p.SkipBinlogData.BinlogPosition >= data.BinlogPosition {
			return true
		}
		if This.p.SkipBinlogData.BinlogFileNum > data.BinlogFileNum {
			return true
		}
	}
	return false
}

func checkOpMap(opMap map[interface{}]*opLog, key interface{}, EvenType string) bool {
	if key == "" {
		return false
	}
	if _, ok := opMap[key]; ok {
		return true
	}
	return false
}

func setOpMapVal(opMap map[interface{}]*opLog, key interface{}, data *[]dbDriver.Value, EventType string) {
	opMap[key] = &opLog{Data: data, EventType: EventType}
}
