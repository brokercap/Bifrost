package src

import (
	"database/sql/driver"
	dbDriver "database/sql/driver"
	"encoding/json"
	"fmt"
	"log"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
)

const VERSION = "v2.1.1"
const BIFROST_VERION = "v2.1.0"

var l sync.RWMutex

type TableDataStruct struct {
	Data       []*pluginDriver.PluginDataType
	CommitData []*pluginDriver.PluginDataType // commit 提交的数据列表，Data 每 BatchSize 数据量划分为一个最后提交的commit
}

type fieldStruct struct {
	CK     string
	MySQL  string
	CkType string
}

func init() {
	pluginDriver.Register("clickhouse", NewConn, VERSION, BIFROST_VERION)
}

func NewTableData() *TableDataStruct {
	CommitData := make([]*pluginDriver.PluginDataType, 0)
	CommitData = append(CommitData, nil)
	return &TableDataStruct{
		Data:       make([]*pluginDriver.PluginDataType, 0),
		CommitData: CommitData,
	}
}

func NewConn() pluginDriver.Driver {
	return &Conn{status: "close"}
}

type SyncType string

const (
	SYNCMODE_NORMAL     SyncType = "Normal"
	SYNCMODE_LOG_UPDATE SyncType = "LogUpdate"
	SYNCMODE_LOG_APPEND SyncType = "insertAll"
)

type DDLSupportType struct {
	ColumnAdd      bool
	ColumnModify   bool
	ColumnDrop     bool
	TableRename    bool
	DropDbAndTable bool
	Rruncate       bool
}

type PluginParam struct {
	Field                  []fieldStruct
	BatchSize              int
	CkSchema               string
	CkTable                string
	PriKey                 []fieldStruct
	SyncType               SyncType
	AutoCreateTable        bool
	AutoSchemaPrefix       string
	AutoTablePrefix        string
	NullNotTransferDefault bool //是否将null值强制转成相对应类型的默认值 , false 将 null 转成相对就的 0 或者 "" , true 不进行转换，为了兼容老版本，才反过来的
	BifrostMustBeSuccess   bool // bifrost server 保留,数据是否能丢
	LowerCaseTableNames    int8 // 0 源字段怎么样，就怎么样，1 转成小写，2 全部转成大写; 只对自动建表的功能有效
	//ModifDDLMap            map[string]bool //ddl同步程度选择
	ModifDDLType  *DDLSupportType //ddl同步程度选择
	CkEngine      int
	CkTableEngine string
	CkClusterName string
	// 以上的数据是 界面配置的参数

	// 以下的数据 是插件执行的时候，进行计算而来的
	ckDatakey               string
	ckPriKey                string // ck 主键字段
	ckPriKeyFieldIsInt      bool   // ck 主键存储类型是否为int类型
	mysqlPriKey             string //ck对应 mysql 的主键id
	Data                    *TableDataStruct
	bifrostDataVersionField string                       // 版本记录字段，delete的时候有用
	nowBifrostDataVersion   int64                        // 每次提交的时候都会更新这个版本号，纳秒时间戳
	tableMap                map[string]*PluginParam0     // 需要自动创建ck表结构 创建之后表基本信息
	ckDatabaseMap           map[string]bool              // ck 里,database 列表信息，database name 做为key，用于缓存
	SkipBinlogData          *pluginDriver.PluginDataType // 在执行 skip 的时候 ，进行传入进来的时候需要要过滤的 位点，在每次commit之后，这个数据会被清空
}

type PluginParam0 struct {
	Field            []fieldStruct
	CkSchema         string
	CkTable          string
	CkSchemaAndTable string
}

type Conn struct {
	pluginDriver.PluginDriverInterface
	uri       *string
	status    string
	p         *PluginParam
	conn      *ClickhouseDB
	ckVersion int
	err       error
}

func (This *Conn) Connect() bool {
	if This.conn == nil {
		This.conn = NewClickHouseDBConn(*This.uri)
	}
	This.InitVersion()
	return true
}

func (This *Conn) InitVersion() {
	defer func() {
		if err := recover(); err != nil {
			return
		}
	}()
	if This.conn == nil {
		return
	}
	versionStr := This.conn.GetVersion()
	if versionStr == "" {
		return
	}
	This.ckVersion = This.InitVersion0(versionStr)
}

func (This *Conn) InitVersion0(versionStr string) int {
	versionArr := strings.Split(versionStr, ".")
	var versionInt = 0
	var vArr = make([]int, 4)
	for i, ver := range versionArr {
		v0, err := strconv.Atoi(ver)
		if err != nil {
			log.Printf("clickhouse version:%s to int err: %s", versionStr, err.Error())
			return 0
		}
		if i <= 3 {
			vArr[i] = v0
		}
	}
	versionInt = vArr[0] * 100000000
	versionInt += vArr[1] * 1000000
	versionInt += vArr[2] * 10000
	versionInt += vArr[3]
	return versionInt
}

func (This *Conn) ReConnect() bool {
	This.Close()
	This.Connect()
	if This.conn.err == nil {
		if This.p.AutoCreateTable == true {
			This.p.tableMap = make(map[string]*PluginParam0, 0)
			This.initCkDatabaseMap()
		} else {
			This.getCktFieldType()
		}
	}
	return true
}

func (This *Conn) GetUriExample() string {
	return "tcp://127.0.0.1:9000?username=&password=&compress=true"
}

func (This *Conn) SetOption(uri *string, param map[string]interface{}) {
	This.uri = uri
	return
}

func (This *Conn) CheckUri() error {
	var err error
	This.Connect()
	if This.conn.err != nil {
		return This.conn.err
	}
	var schemaList []string
	func() {
		defer func() {
			return
		}()
		schemaList = This.conn.GetSchemaList()
	}()
	if len(schemaList) == 0 {
		This.conn.Close()
		err = fmt.Errorf("schema count is 0 (not in system)")
	}
	return err
}

func (This *Conn) Open() error {
	This.Connect()
	return nil
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
	This.conn = nil
	This.status = "close"
	return true
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
	if param.CkTable == "" {
		param.AutoCreateTable = true
	}
	if param.AutoCreateTable == false {
		param.ckDatakey = "`" + param.CkSchema + "`.`" + param.CkTable + "`"
		param.ckPriKey = param.PriKey[0].CK
		param.mysqlPriKey = param.PriKey[0].MySQL
	}
	param.Data = NewTableData()
	if param.SyncType == "" {
		param.SyncType = SYNCMODE_NORMAL
	}
	if param.AutoCreateTable == true {
		param.SyncType = SYNCMODE_LOG_APPEND
	}
	if param.ModifDDLType == nil {
		param.ModifDDLType = &DDLSupportType{}
	}
	This.p = &param
	if param.AutoCreateTable == false {
		This.getCktFieldType()
	I:
		for _, v := range This.p.Field {
			if v.CK == "{$BifrostDataVersion}" {
				switch v.CkType {
				case "Int64", "Nullable(Int64)", "UInt64", "Nullable(UInt64)":
					This.p.bifrostDataVersionField = v.CK
					break I
				default:
					break
				}
			}
		}
	} else {
		param.tableMap = make(map[string]*PluginParam0, 0)
		This.initCkDatabaseMap()
	}
	return &param, nil
}

func (This *Conn) SetParam(p interface{}) (interface{}, error) {
	if p == nil {
		return nil, fmt.Errorf("param is nil")
	}
	switch p.(type) {
	case *PluginParam:
		This.p = p.(*PluginParam)
		if This.p.CkEngine == 0 { //表示是旧版本升级上来的(旧版本只有单机情况) 强制赋值为1 表示单机  2表示 集群
			This.p.CkEngine = 1
			//因为旧版本ddl默认是全部开启的  所以ddl同步 新版在这默认全部开启以兼容老版本
			This.p.ModifDDLType.ColumnAdd = true
			This.p.ModifDDLType.ColumnModify = true
			This.p.ModifDDLType.TableRename = true

			This.p.ModifDDLType.ColumnDrop = false
			This.p.ModifDDLType.DropDbAndTable = false
			This.p.ModifDDLType.Rruncate = false

		}
		return This.p, nil
	default:
		tmpP, err := This.GetParam(p)
		if err != nil {
			return nil, err
		}

		if tmpP.CkEngine == 0 { //表示是旧版本升级上来的(旧版本只有单机情况) 强制赋值为1 表示单机  2表示 集群
			tmpP.CkEngine = 1
			//因为旧版本ddl默认是全部开启的  所以ddl同步 新版在这默认全部开启以兼容老版本
			This.p.ModifDDLType.ColumnAdd = true
			This.p.ModifDDLType.ColumnModify = true
			This.p.ModifDDLType.TableRename = true

			This.p.ModifDDLType.ColumnDrop = false
			This.p.ModifDDLType.DropDbAndTable = false
			This.p.ModifDDLType.Rruncate = false
		}
		return tmpP, nil
	}
}

func (This *Conn) getCktFieldType() {
	defer func() {
		if err := recover(); err != nil {
			This.conn.err = fmt.Errorf(fmt.Sprint(err))
			log.Println(string(debug.Stack()))
		}
	}()
	if This.p == nil {
		return
	}

	ckFields := This.conn.GetTableFields(This.p.CkSchema, This.p.CkTable)
	if This.conn.err != nil {
		This.err = This.conn.err
		return
	}
	if len(ckFields) == 0 {
		return
	}
	ckFieldsMap := make(map[string]string)
	for _, v := range ckFields {
		ckFieldsMap[v.Name] = v.Type
		if v.Name == This.p.ckPriKey {
			switch v.Type {
			case "Int8", "Nullable(Int8)", "UInt8", "Nullable(UInt8)", "Int16", "Nullable(Int16)", "UInt16", "Nullable(UInt16)", "Int32", "Nullable(Int32)", "UInt32", "Nullable(UInt32)", "Int64", "Nullable(Int64)", "UInt64", "Nullable(UInt64)":
				This.p.ckPriKeyFieldIsInt = true
			default:
				This.p.ckPriKeyFieldIsInt = false
			}
		}
	}

	// 有一种可能，就是目标库，把某一个字段删除了，但是绑定的字段中，还存在这个字段，为了避免报错，以ck表中有的字段，并且配置了绑定的字段为准
	Fields := make([]fieldStruct, 0)
	for k, v := range This.p.Field {
		if _, ok := ckFieldsMap[v.CK]; ok {
			This.p.Field[k].CkType = ckFieldsMap[v.CK]
			Fields = append(Fields, This.p.Field[k])
		}
	}
	This.p.Field = Fields
}

func (This *Conn) GetFieldName(Name string) string {
	switch This.p.LowerCaseTableNames {
	case 0:
		return Name
	case 1:
		return strings.ToLower(Name)
	case 2:
		return strings.ToUpper(Name)
	default:
		return Name
	}
}

func (This *Conn) GetSchemaName(Name string) (SchemaName string) {
	if This.p.CkSchema == "" {
		SchemaName = This.p.AutoSchemaPrefix + This.GetFieldName(Name)
	} else {
		SchemaName = This.p.CkSchema
	}
	return
}

func (This *Conn) GetTableName(Name string) (TableName string) {
	if This.p.CkTable == "" {
		TableName = This.p.AutoTablePrefix + This.GetFieldName(Name)
	} else {
		TableName = Name
	}
	return
}

// 通过数据自动创建 ck 库
func (This *Conn) CreateCkDatabase(SchemaName string) (err error) {
	if _, ok := This.p.ckDatabaseMap[SchemaName]; ok {
		return nil
	}
	sql := This.TransferToCreateDatabaseSql(SchemaName)
	This.conn.err = This.conn.Exec(sql, []driver.Value{})
	if This.conn.err != nil {
		return This.conn.err
	}
	This.p.ckDatabaseMap[SchemaName] = true
	return nil
}

// 通过数据自动创建 ck 表
func (This *Conn) CreateCkTable(data *pluginDriver.PluginDataType) (ckField []fieldStruct, err error) {
	sql, disSql, viewSql, ckField2 := This.TransferToCreateTableSql(data)

	switch This.p.CkEngine {
	case 1:
		if sql == "" {
			return nil, nil
		}

		This.conn.err = This.conn.Exec(sql, []driver.Value{})
		if This.conn.err != nil {
			return nil, This.conn.err
		}
	case 2:
		if sql == "" || disSql == "" || viewSql == "" {
			return nil, nil
		}
		//创建本地表
		This.conn.err = This.conn.Exec(sql, []driver.Value{})
		if This.conn.err != nil {
			return nil, This.conn.err
		}
		//创建分布式表
		This.conn.err = This.conn.Exec(disSql, []driver.Value{})
		if This.conn.err != nil {
			return nil, This.conn.err
		}

		// 创建 pview
		This.conn.err = This.conn.Exec(viewSql, []driver.Value{})
		if This.conn.err != nil {
			return nil, This.conn.err
		}
	}
	return ckField2, nil
}

// 获取表结构信息，只限自动创建表的同步逻辑中
func (This *Conn) getAutoCreateCkTableFieldType(SchemaName, TableName string, data map[string]interface{}) (ckField []fieldStruct, err error) {
	ckFields := This.conn.GetTableFields(SchemaName, TableName)
	if len(ckFields) == 0 {
		return nil, fmt.Errorf("don't find SchemaName:%s TableName %s", SchemaName, TableName)
	}
	var ok bool
	ckField2 := make([]fieldStruct, 0)

	// 将 mysql 里的行数据,转成 小写key和字段名映射关系,用于判断 目标表里的字段和源表字段，能否对应上
	// 比如目标表是 test_name ,但是源表是 Test_Name
	var mysqlSourceData = make(map[string]string, 0)
	for fieldName, _ := range data {
		mysqlSourceData[strings.ToLower(fieldName)] = fieldName
	}
	for _, v := range ckFields {
		var MySQLFieldName string
		// 假如 ck 表的中字段名,并不在传过来的数据中,则认为 源端中没有这个字段，
		// 有一些保留字段,则自动用标签替换处理
		// 这里将 ck 字段名也强转成小写作为key，用于判断是否和 mysql 里的字段,判断是否相等，这里不区分大小写，防止用户ck 建表是小写，但是 MySQL 里字段名是大写
		var ckFieldName = strings.ToLower(v.Name)
		if _, ok = mysqlSourceData[ckFieldName]; !ok {
			switch v.Name {
			case "bifrost_data_version":
				MySQLFieldName = "{$BifrostDataVersion}"
				break
			case "binlog_event_type":
				MySQLFieldName = "{$EventType}"
				break
			case "binlog_timestamp", "binlogtimestamp":
				MySQLFieldName = "{$BinlogTimestamp}"
				break
			case "binlogfilenum":
				MySQLFieldName = "{$BinlogFileNum}"
				break
			case "binlogposition":
				MySQLFieldName = "{$BinlogPosition}"
				break
			default:
				MySQLFieldName = ""
				break
			}
		} else {
			MySQLFieldName = mysqlSourceData[ckFieldName]
		}
		ckField2 = append(ckField2, fieldStruct{CK: v.Name, MySQL: MySQLFieldName, CkType: v.Type})
	}
	return ckField2, nil
}

// 假如ck中没有表,则自动根据源端数据类型 自动创建ck表结构
func (This *Conn) initAutoCreateCkTableFieldType(data *pluginDriver.PluginDataType) (*PluginParam0, error) {
	defer func() {
		if err0 := recover(); err0 != nil {
			This.conn.err = fmt.Errorf(fmt.Sprint(err0))
		}
	}()
	var err error
	var SchemaName string
	var TableName string

	switch This.p.CkEngine {
	case 1:
		SchemaName = This.GetSchemaName(data.SchemaName)
	case 2:
		SchemaName = This.GetSchemaName(data.SchemaName)
	}

	This.CreateCkDatabase(SchemaName)
	if This.conn.err != nil {
		return nil, This.conn.err
	}

	switch This.p.CkEngine {
	case 1:
		TableName = This.GetTableName(data.TableName)
	case 2:
		TableName = This.GetTableName(data.TableName) + "_all"
	}

	key := "`" + SchemaName + "`.`" + TableName + "`"
	if _, ok := This.p.tableMap[key]; ok {
		return This.p.tableMap[key], nil
	}
	var ckField []fieldStruct
	//获取 ck mysql 字段对应关系   {CK: v.Name, MySQL: MySQLFieldName, CkType: v.Type}
	ckField, _ = This.getAutoCreateCkTableFieldType(SchemaName, TableName, data.Rows[len(data.Rows)-1])
	if ckField == nil || len(ckField) == 0 {
		ckField, err = This.CreateCkTable(data)
	}
	if err != nil {
		return nil, err
	}
	if ckField == nil {
		return nil, nil
	}
	p0 := &PluginParam0{
		Field:            ckField,
		CkSchema:         SchemaName,
		CkTable:          TableName,
		CkSchemaAndTable: key,
	}
	This.p.tableMap[key] = p0
	return p0, nil
}

// 查出ck 里所有database,放到 map 中，用于缓存
func (This *Conn) initCkDatabaseMap() {
	This.p.ckDatabaseMap = make(map[string]bool, 0)
	defer func() {
		if err := recover(); err != nil {
			return
		}
	}()
	SchemaList := This.conn.GetSchemaList()
	for _, Name := range SchemaList {
		This.p.ckDatabaseMap[Name] = true
	}
	return
}

// 将数据放到 list 里,假如满足条件，则合并提交数据到ck里
func (This *Conn) sendToCacheList(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	var n int
	if retry == false {
		This.p.Data.Data = append(This.p.Data.Data, data)
	}
	n = len(This.p.Data.Data)
	if This.p.BatchSize <= n {
		return This.AutoCommit()
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
	if This.p.AutoCreateTable == false || data.Query == "" {
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
			SchemaName, TableName, newSql, newLocalSql, newDisSql, newViewSql := This.TranferQuerySql(data)
			switch This.p.CkEngine {
			case 1: //单机
				if newSql == "" {
					return data, nil, This.conn.err
				}
			case 2: //集群
				if newLocalSql == "" || newDisSql == "" || newViewSql == "" {
					return data, nil, This.conn.err
				}
			}

			// DATABASE 级别操作 不需要验证下面的存在逻辑。 已经添加了 IF EXISTS
			switch This.p.CkEngine {
			case 1: //单机
				if strings.Contains(newSql, "DATABASE") {
					This.conn.err = This.conn.Exec(newSql, []driver.Value{})
					if This.conn.err != nil {
						log.Printf("plugin clickhouse, exec sql:%s err:%s", newSql, This.conn.err)
						return nil, data, This.conn.err
					}
				}
			case 2: //集群
				if strings.Contains(newLocalSql, "DATABASE") { //DATABASE 执行一个 一次删除就行了
					This.conn.err = This.conn.Exec(newLocalSql, []driver.Value{})
					if This.conn.err != nil {
						log.Printf("plugin clickhouse, exec sql:%s err:%s", newLocalSql, This.conn.err)
						return nil, data, This.conn.err
					}
				}
			}

			// 非 database 级别，表级别操作，检查表是否存在
			b, _ := This.CheckTableExsit(SchemaName, TableName)
			if b == false {
				return data, nil, nil
			}
			if This.conn == nil || This.conn.err != nil {
				This.ReConnect()
			}
			if This.conn.err != nil {
				return nil, nil, This.conn.err
			}
			func() {
				defer func() {
					if err := recover(); err != nil {
						This.conn.err = fmt.Errorf("ddl exec err:%s", fmt.Sprint(err))
					}
				}()
			}()

			switch This.p.CkEngine {
			case 1: //单机
				This.conn.err = This.conn.Exec(newSql, []driver.Value{})
				if This.conn.err != nil {
					log.Printf("plugin mysql, exec sql:%s err:%s", newSql, This.conn.err)
					return nil, data, This.conn.err
				}
			case 2: //集群
				This.conn.err = This.conn.Exec(newLocalSql, []driver.Value{})
				if This.conn.err != nil {
					log.Printf("plugin mysql, exec sql:%s err:%s", newLocalSql, This.conn.err)
					return nil, data, This.conn.err
				}
				This.conn.err = This.conn.Exec(newDisSql, []driver.Value{})
				if This.conn.err != nil {
					log.Printf("plugin mysql, exec sql:%s err:%s", newDisSql, This.conn.err)
					return nil, data, This.conn.err
				}

				for _, v := range strings.Split(newViewSql, ";") {
					This.conn.err = This.conn.Exec(v, []driver.Value{})
					if This.conn.err != nil {
						log.Printf("plugin mysql, exec sql:%s err:%s", v, This.conn.err)
						return nil, data, This.conn.err
					}
				}
			}

			// 清掉缓存，下一次数据操作的时候，再从 ck 里读取
			key := "`" + SchemaName + "`.`" + TableName + "`"
			delete(This.p.tableMap, key)
			break
		}
	}
	return
}

func (This *Conn) CheckTableExsit(SchemaName, TableName string) (bool, error) {
	if This.conn.err != nil {
		return false, This.conn.err
	}
	key := "`" + SchemaName + "`.`" + TableName + "`"
	if _, ok := This.p.tableMap[key]; ok {
		return true, nil
	}
	ckFields := This.conn.GetTableFields(SchemaName, TableName)
	if len(ckFields) == 0 {
		return false, nil
	}
	return true, nil
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

func (This *Conn) TimeOutCommit() (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return This.AutoCommit()
}

// 获取 sql stmt
func (This *Conn) getStmt(Type string) dbDriver.Stmt {
	var stmt dbDriver.Stmt
	switch Type {
	case "insert":
		fields := ""
		values := ""
		for _, v := range This.p.Field {
			if fields == "" {
				fields = v.CK
				values = "?"
			} else {
				fields += "," + v.CK
				values += ",?"
			}
		}
		sql := "INSERT INTO " + This.p.ckDatakey + " (" + fields + ") VALUES (" + values + ")"
		stmt, This.conn.err = This.conn.conn.Prepare(sql)
		if This.conn.err != nil {
			log.Println("clickhouse getStmt insert err:", This.conn.err, "sql:", sql)
		}
		break
	case "delete":
		where := ""
		for _, v := range This.p.PriKey {
			if where == "" {
				where = v.CK + "=?"
			} else {
				where += " AND " + v.CK + "=?"
			}
		}
		stmt, This.conn.err = This.conn.conn.Prepare("ALTER TABLE " + This.p.ckDatakey + " DELETE WHERE " + where)
		if This.conn.err != nil {
			log.Println("clickhouse getStmt delete err:", This.conn.err)
		}
		break
	default:
		//默认是传sql进来
		stmt, This.conn.err = This.conn.conn.Prepare(Type)
		if This.conn.err != nil {
			log.Println("clickhouse getStmt err:", This.conn.err, " sql:", Type)
		}
		break
	}

	if This.conn.err != nil {
		return nil
	}
	return stmt
}

// 将 使用的标签数据转换成相对应的值
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
	case "{$BifrostDataVersion}":
		This.p.nowBifrostDataVersion++
		return This.p.nowBifrostDataVersion
		break
	default:
		return pluginDriver.TransfeResult(key, data, index, true)
		break
	}
	return ""
}

// 合并数据，提交到  ck里
func (This *Conn) AutoCommit() (LastSuccessCommitData *pluginDriver.PluginDataType, ErrData *pluginDriver.PluginDataType, e error) {
	defer func() {
		if err := recover(); err != nil {
			e = fmt.Errorf(string(debug.Stack()))
			This.conn.err = e
		}
	}()
	if This.conn == nil || This.conn.err != nil {
		This.ReConnect()
	}
	if This.conn.err != nil {
		log.Println(" This.conn.err:", This.conn.err)
		return nil, nil, This.conn.err
	}
	if This.err != nil {
		log.Println("This.err:", This.err)
	}
	n := len(This.p.Data.Data)
	if n == 0 {
		return nil, nil, nil
	}
	i := time.Now().UnixNano()
	if i >= This.p.nowBifrostDataVersion {
		This.p.nowBifrostDataVersion = i
	}
	if n > This.p.BatchSize {
		n = This.p.BatchSize
	}
	list := This.p.Data.Data[:n]
	if This.p.AutoCreateTable == true {
		ErrData = This.AutoCreateTableCommit(list, n)
	} else {
		ErrData = This.NotCreateTableCommit(list, n)
	}
	// 假如数据不能丢，才需要 判断 是否有err，如果可以丢，直接错过数据
	if This.err != nil && This.p.BifrostMustBeSuccess {
		return nil, ErrData, This.err
	}
	var binlogEvent *pluginDriver.PluginDataType
	if len(This.p.Data.Data) <= int(This.p.BatchSize) {
		binlogEvent = This.p.Data.CommitData[0]
		//log.Println("binlogEvent:",*binlogEvent)
		This.p.Data = NewTableData()
	} else {
		This.p.Data.Data = This.p.Data.Data[n:]
		if len(This.p.Data.CommitData) > 0 {
			binlogEvent = This.p.Data.CommitData[0]
			This.p.Data.CommitData = This.p.Data.CommitData[1:]
		}
	}
	This.p.SkipBinlogData = nil
	return binlogEvent, nil, nil
}

// 自动创建表的提交
func (This *Conn) AutoCreateTableCommit(list []*pluginDriver.PluginDataType, n int) (errData *pluginDriver.PluginDataType) {
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
		p, err := This.initAutoCreateCkTableFieldType(data[0])
		if p == nil && err == nil {
			continue
		}
		if err != nil {
			This.err = err
			break
		}
		This.p.Field = p.Field
		This.p.ckDatakey = p.CkSchemaAndTable
		var tx driver.Tx
		tx, This.conn.err = This.conn.conn.Begin()
		if This.conn.err != nil {
			This.err = This.conn.err
		}
		errData = This.CommitLogMod_Append(data, len(data))
		//假如连接本身有异常的情况下,则执行 rollback
		if This.conn.err != nil {
			tx.Rollback()
			This.err = This.conn.err
			break
		}
		// tx.Rollback() 会造成连接异常，因为是追加模式 ，所以我们采用 commit ，数据不会有问题
		This.conn.err = tx.Commit()
		if This.conn.err != nil {
			This.err = This.conn.err
		}
		if This.err != nil {
			break
		}
	}
	return
}

// 非自动创建表的提交
func (This *Conn) NotCreateTableCommit(list []*pluginDriver.PluginDataType, n int) (errData *pluginDriver.PluginDataType) {
	var tx driver.Tx
	tx, This.conn.err = This.conn.conn.Begin()
	if This.conn.err != nil {
		return
	}
	switch This.p.SyncType {
	case SYNCMODE_LOG_APPEND:
		errData = This.CommitLogMod_Append(list, n)
		break
	case SYNCMODE_NORMAL, SYNCMODE_LOG_UPDATE:
		errData = This.CommitNormal(list, n)
		break
	default:
		This.err = fmt.Errorf("clickhoue SyncType:%s ,not found! ", This.p.SyncType)
		break
	}
	if This.conn.err != nil {
		tx.Rollback()
		This.err = This.conn.err
		return
	}
	This.conn.err = tx.Commit()
	return
}

// 设置跳过的位点
func (This *Conn) Skip(SkipData *pluginDriver.PluginDataType) error {
	This.p.SkipBinlogData = SkipData
	return nil
}

func (This *Conn) CheckDataSkip(data *pluginDriver.PluginDataType) bool {
	if This.p.SkipBinlogData != nil {
		if This.p.SkipBinlogData.BinlogFileNum > data.BinlogFileNum {
			return true
		}
		if This.p.SkipBinlogData.BinlogFileNum == data.BinlogFileNum && This.p.SkipBinlogData.BinlogPosition >= data.BinlogPosition {
			return true
		}
	}
	return false
}
