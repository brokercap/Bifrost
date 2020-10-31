package src

import (
	dbDriver "database/sql/driver"
	"encoding/json"
	"fmt"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"log"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)


const VERSION  = "v1.6.0"
const BIFROST_VERION = "v1.6.0"

type TableDataStruct struct {
	Data 			[]*pluginDriver.PluginDataType
	CommitData 		[]*pluginDriver.PluginDataType		// commit 提交的数据列表，Data 每 BatchSize 数据量划分为一个最后提交的commit
}

func init(){
	pluginDriver.Register("mysql",NewConn,VERSION,BIFROST_VERION)
}

type EventType int8

const (
	INSERT EventType = 0
	UPDATE EventType = 1
	DELETE EventType = 2
	REPLACE_INSERT EventType = 3
	SQLTYPE EventType = 4
)

type SyncMode string
const (
	SYNCMODE_NORMAL SyncMode = "Normal"
	SYNCMODE_LOG_UPDATE SyncMode = "LogUpdate"
	SYNCMODE_LOG_APPEND SyncMode = "LogAppend"
)

type fieldStruct struct {
	ToField                string
	FromMysqlField         string
	ToFieldType            string
	ToFieldDefault         *string
}


func NewTableData() *TableDataStruct {
	CommitData := make([]*pluginDriver.PluginDataType,0)
	CommitData = append(CommitData,nil)
	return &TableDataStruct{
		Data:		make([]*pluginDriver.PluginDataType,0),
		CommitData:	CommitData,
	}
}

type Conn struct {
	uri    	*string
	status  string
	p		*PluginParam
	conn    *mysqlDB
	err 	error
}

type PluginParam struct {
	Field 			[]fieldStruct
	BatchSize      	int
	Schema			string
	Table			string
	NullTransferDefault bool  //是否将null值强制转成相对应类型的默认值
	SyncMode		SyncMode
	BifrostMustBeSuccess	bool  // bifrost server 保留,数据是否能丢

	Datakey			string
	replaceInto		bool  // 记录当前表是否有replace into操作
	PriKey			[]fieldStruct
	toPriKey		string   // toMysql 主键字段
	mysqlPriKey		string  //	对应 from mysql 的主键id
	Data			*TableDataStruct
	fieldCount		int
	stmtArr			[]dbDriver.Stmt
	SkipBinlogData	*pluginDriver.PluginDataType		// 在执行 skip 的时候 ，进行传入进来的时候需要要过滤的 位点，在每次commit之后，这个数据会被清空
}


func NewConn() pluginDriver.Driver {
	return &Conn{status:"close",}
}

func (This *Conn) SetOption(uri *string,param map[string]interface{}) {
	This.uri = uri
	return
}


func (This *Conn) Open() error{
	This.Connect()
	return nil
}

func (This *Conn) CheckUri() error{
	This.Connect()
	if This.err != nil{
		return This.err
	}
	if This.conn == nil{
		This.Close()
		return fmt.Errorf("connect")
	}

	var schemaList []string
	func(){
		defer func() {
			return
		}()
		schemaList = This.conn.GetSchemaList()
	}()
	if len(schemaList) == 0{
		This.Close()
		return fmt.Errorf("schema count is 0 (not in system)")
	}
	return nil
}

func (This *Conn) GetUriExample() string{
	return "root:root@tcp(127.0.0.1:3306)/test"
}

func (This *Conn) GetParam(p interface{}) (*PluginParam,error){
	s,err := json.Marshal(p)
	if err != nil{
		return nil,err
	}
	var param PluginParam
	err2 := json.Unmarshal(s,&param)
	if err2 != nil{
		return nil,err2
	}
	if param.BatchSize == 0{
		param.BatchSize = 500
	}
	param.Data = NewTableData()
	param.Datakey = "`"+param.Schema+"`.`"+param.Table+"`"
	param.toPriKey = param.PriKey[0].ToField
	param.mysqlPriKey = param.PriKey[0].FromMysqlField
	param.stmtArr = make([]dbDriver.Stmt,4)
	if param.SyncMode == ""{
		param.SyncMode = SYNCMODE_NORMAL
	}
	This.p = &param
	This.getCktFieldType()
	return &param,nil
}

func (This *Conn) SetParam(p interface{}) (interface{},error){
	if p == nil{
		return nil,fmt.Errorf("param is nil")
	}
	switch p.(type) {
	case *PluginParam:
		This.p = p.(*PluginParam)
		return p,nil
	default:
		return This.GetParam(p)
	}
}

func (This *Conn) getCktFieldType() {
	defer func() {
		if err := recover();err !=nil{
			log.Println(string(debug.Stack()))
			This.conn.err = fmt.Errorf(string(debug.Stack()))
		}
	}()
	if This.p == nil{
		return
	}

	fields := This.conn.GetTableFields(This.p.Schema,This.p.Table)
	if This.conn.err != nil{
		This.err = This.conn.err
		return
	}
	if len(fields) == 0{
		return
	}
	ckFieldsMap := make(map[string]TableStruct)
	for _,v:=range fields{
		ckFieldsMap[v.COLUMN_NAME] = v
	}
	list := make([]fieldStruct,0)
	for k,v:=range This.p.Field{
		This.p.Field[k].ToFieldType = ckFieldsMap[v.ToField].DATA_TYPE
		if strings.ToLower(ckFieldsMap[v.ToField].EXTRA) == "auto_increment"{
			This.p.Field[k].ToFieldDefault = nil
			if v.FromMysqlField != ""{
				list = append(list,This.p.Field[k])
			}
		}else {
			This.p.Field[k].ToFieldDefault = ckFieldsMap[v.ToField].COLUMN_DEFAULT
			list = append(list,This.p.Field[k])
		}
	}
	This.p.Field = list
	This.p.fieldCount = len(list)
}

func (This *Conn) Connect() bool {
	This.conn = NewMysqlDBConn(*This.uri)
	if This.conn.err != nil{
		This.conn.conn.Exec("SET NAMES UTF8",[]dbDriver.Value{})
	}
	return true
}

func (This *Conn) ReConnect() bool {
	if This.conn != nil{
		defer func() {
			if err := recover();err !=nil{
				This.conn.err = fmt.Errorf(fmt.Sprint(err)+" debug:"+string(debug.Stack()))
			}
		}()
		This.closeStmt0()
		This.conn.Close()
	}
	This.Connect()
	if This.conn.err == nil{
		This.getCktFieldType()
	}
	return  true
}

func (This *Conn) StmtClose() {
	for k,stmt := range This.p.stmtArr{
		if stmt != nil{
			func(){
				defer func() {
					if err := recover();err!=nil{
						This.conn.err = fmt.Errorf("StmtClose err:%s",fmt.Sprint(err))
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
		func(){
			defer func() {
				if err := recover();err != nil {
					return
				}
			}()
			This.conn.Close()
		}()
	}
	return true
}

func (This *Conn) sendToCacheList(data *pluginDriver.PluginDataType,retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error){
	var n int
	if retry == false {
		This.p.Data.Data = append(This.p.Data.Data, data)
	}
	n = len(This.p.Data.Data)
	if This.p.BatchSize <= n{
		return This.AutoCommit()
	}
	return nil,nil,nil
}

func (This *Conn) Insert(data *pluginDriver.PluginDataType,retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return This.sendToCacheList(data,retry)
}

func (This *Conn) Update(data *pluginDriver.PluginDataType,retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return This.sendToCacheList(data,retry)
}

func (This *Conn) Del(data *pluginDriver.PluginDataType,retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return This.sendToCacheList(data,retry)
}

func (This *Conn) Query(data *pluginDriver.PluginDataType,retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return nil,nil,nil
}

func (This *Conn) Commit(data *pluginDriver.PluginDataType,retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	n := len(This.p.Data.Data)
	if n == 0 {
		return data,nil,nil
	}
	n0 := n / This.p.BatchSize
	if len(This.p.Data.CommitData) - 1 < n0 {
		This.p.Data.CommitData = append(This.p.Data.CommitData,data)
	}else{
		This.p.Data.CommitData[n0] = data
	}
	return nil, nil, nil
}

func (This *Conn) TimeOutCommit() (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType,error) {
	return This.AutoCommit()
}

func (This *Conn) getMySQLData(data *pluginDriver.PluginDataType,index int,key string) interface{} {
	if key == ""{
		return nil
	}
	if _,ok := data.Rows[index][key];ok {
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
		return  pluginDriver.TransfeResult(key,data,index)
		break
	}
	return ""
}

// 设置跳过的位点
func (This *Conn) Skip (SkipData *pluginDriver.PluginDataType) error {
	This.p.SkipBinlogData = SkipData
	return nil
}

func (This *Conn) AutoCommit() (LastSuccessCommitData *pluginDriver.PluginDataType,ErrData *pluginDriver.PluginDataType,e error) {
	defer func() {
		if err := recover();err != nil{
			e = fmt.Errorf(string(debug.Stack()))
			log.Println(string(debug.Stack()))
			This.conn.err = e
		}
	}()
	n := len(This.p.Data.Data)
	if n == 0{
		return nil,nil,nil
	}
	if This.conn.err != nil {
		This.ReConnect()
	}
	if This.conn.err != nil {
		return nil,nil,This.conn.err
	}
	if n > This.p.BatchSize{
		n = This.p.BatchSize
	}
	list := This.p.Data.Data[:n]

	This.conn.err = This.conn.Begin()
	if This.conn.err != nil{
		return nil,nil,This.conn.err
	}
	var errData *pluginDriver.PluginDataType
	switch This.p.SyncMode {
	case SYNCMODE_NORMAL:
		errData = This.CommitNormal(list)
		break
	case SYNCMODE_LOG_UPDATE:
		errData = This.CommitLogMod_Update(list)
		break
	case SYNCMODE_LOG_APPEND:
		errData = This.CommitLogMod_Append(list)
		break
	default:
		This.err = fmt.Errorf("同步模式ERROR:%s",This.p.SyncMode)
		break
	}

	if This.conn.err != nil {
		This.err = This.conn.err
		//log.Println("plugin mysql conn.err",This.err)
		return nil,errData,This.err
	}
	if This.err != nil{
		This.conn.err = This.conn.Rollback()
		log.Println("plugin mysql err",This.err)
		return nil,errData,This.err
	}

	err2 := This.conn.Commit()
	This.StmtClose()
	if err2 != nil{
		This.conn.err = err2
		return nil,nil,This.conn.err
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
	This.p.SkipBinlogData = nil
	return binlogEvent,nil,nil
}

func (This *Conn) dataTypeTransfer(data interface{},fieldName string,toDataType string,defaultVal *string) (v dbDriver.Value,e error) {
	defer func() {
		if err := recover();err != nil{
			log.Println("plugin mysql dataTypeTransfer:",fmt.Sprint(err))
			log.Println(string(debug.Stack()))
			e = fmt.Errorf(fieldName+" "+fmt.Sprint(err))
		}
	}()
	if data == nil {
		if This.p.NullTransferDefault == false {
			if defaultVal == nil{
				v = nil
				return
			}else{
				// 这里要判断 是不是 bit 类型，因为 bit 类型在传输值 上 必须 为 int64 类型，不能是字符串
				if toDataType == "bit"{
					v,_ = strconv.ParseInt(*defaultVal,10,64)
				}else{
					v = *defaultVal
				}
				return
			}
		}else{
			//假如配置是强制转成默认值
			switch toDataType {
			case "int","tinyint","smallint","mediumint","bigint","bool":
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
			case "float","double","decimal","number","point":
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
		if data.(bool) == false{
			data = "0"
		}else{
			data = "1"
		}
		break
	default:
		break
	}
	switch toDataType {
	case "bool":
		switch fmt.Sprint(data) {
		case "0","":
			v = "0"
			break
		default:
			v = "1"
		}
		break
	case "bit":
		switch data.(type) {
		case string:
			v, _ = strconv.ParseInt(data.(string),10,64)
			break
		case int64:
			v = data.(int64)
		default:
			v, _ = strconv.ParseInt(fmt.Sprint(data),10,64)
			break
		}
		break
	case "set":
		switch data.(type) {
		case []string,[]interface{}:
			v = strings.Replace(strings.Trim(fmt.Sprint(data), "[]"), " ", ",", -1)
			break
		default:
			v = fmt.Sprint(data)
			break
		}
		break
	case "json":
		switch reflect.TypeOf(data).Kind() {
		case reflect.Array,reflect.Slice,reflect.Map:
			var c []byte
			c,e = json.Marshal(data)
			if e != nil{
				return
			}
			v = string(c)
			break
		default:
			e = fmt.Errorf("field:%s ,data source type: %s, is not object or array, s ",fieldName,reflect.TypeOf(data).Kind().String())
		}
		break
	default:
		switch reflect.TypeOf(data).Kind() {
		case reflect.Array,reflect.Slice,reflect.Map:
			var c []byte
			c,e = json.Marshal(data)
			if e != nil{
				e = fmt.Errorf("field:%s ,data source type: %s , json.Marshal err: %s ",fieldName,reflect.TypeOf(data).Kind().String(),e.Error())
				return
			}
			v = string(c)
			break
		default:
			v = fmt.Sprint(data)
		}
		break
	}
	return
}


func (This *Conn) getStmt(Type EventType) dbDriver.Stmt{
	if This.p.stmtArr[Type] != nil{
		return This.p.stmtArr[Type]
	}
	switch Type {
	case REPLACE_INSERT:
		fields := ""
		values := ""
		for _,v:= range This.p.Field{
			if fields == ""{
				fields = "`"+v.ToField+"`"
				values = "?"
			}else{
				fields += ",`"+v.ToField+"`"
				values += ",?"
			}
		}
		sql := "REPLACE INTO "+This.p.Datakey+" ("+fields+") VALUES ("+values+")"
		This.p.stmtArr[Type],This.conn.err = This.conn.conn.Prepare(sql)
		if This.conn.err != nil{
			log.Println("mysql getStmt REPLACE_INSERT err:",This.conn.err,sql)
		}
		break
	case INSERT:
		fields := ""
		values := ""
		for _,v:= range This.p.Field{
			if fields == ""{
				fields = "`"+v.ToField+"`"
				values = "?"
			}else{
				fields += ",`"+v.ToField+"`"
				values += ",?"
			}
		}
		sql := "INSERT INTO "+This.p.Datakey+" ("+fields+") VALUES ("+values+")"
		This.p.stmtArr[Type],This.conn.err = This.conn.conn.Prepare(sql)
		if This.conn.err != nil{
			log.Println("mysql getStmt INSERT err:",This.conn.err,sql)
		}
		break
	case DELETE:
		where := ""
		for _,v:= range This.p.PriKey{
			if where == ""{
				where = "`"+v.ToField+"`=?"
			}else{
				where += " AND `"+v.ToField+"`=?"
			}
		}
		This.p.stmtArr[Type],This.conn.err = This.conn.conn.Prepare("DELETE FROM "+This.p.Datakey+" WHERE "+where)
		if This.conn.err != nil{
			log.Println("mysql getStmt DELETE err:",This.conn.err)
		}
		break
	case UPDATE:
		fields := ""
		values := ""
		fields2 := ""
		for _,v:= range This.p.Field{
			if fields == ""{
				fields = "`"+v.ToField+"`"
				values = "?"
				fields2 = "`"+v.ToField+"`=?"
			}else{
				fields += ",`"+v.ToField+"`"
				values += ",?"
				fields2 += ",`"+v.ToField+"`=?"
			}
		}
		sql := "INSERT INTO "+This.p.Datakey+" ("+fields+") VALUES ("+values+") ON DUPLICATE KEY UPDATE "+fields2
		This.p.stmtArr[Type],This.conn.err = This.conn.conn.Prepare(sql)
		if This.conn.err != nil{
			log.Println("mysql getStmt INSERT ON DUPLICATE KEY UPDATE err:",This.conn.err,sql)
		}
		break
	}

	return This.p.stmtArr[Type]
}

func (This *Conn) closeStmt0(){
	for k,_ := range This.p.stmtArr{
		This.p.stmtArr[k] = nil
	}
}

func (This *Conn) CheckDataSkip(data *pluginDriver.PluginDataType) bool {
	if This.p.SkipBinlogData != nil && This.p.SkipBinlogData.BinlogFileNum == data.BinlogFileNum && This.p.SkipBinlogData.BinlogPosition == data.BinlogPosition {
		return true
	}
	return false
}

func checkOpMap(opMap map[interface{}]*opLog,key interface{}, EvenType string) bool {
	if _,ok := opMap[key];ok{
		return true
	}
	return false
}

func setOpMapVal(opMap map[interface{}]*opLog,key interface{},data *[]dbDriver.Value,EventType string) {
	opMap[key] = &opLog{Data:data,EventType:EventType}
}

