package src

import (
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"sync"
	"encoding/json"
	"fmt"
	dbDriver "database/sql/driver"
	"log"
	"strconv"
	"strings"
	"runtime/debug"
)


const VERSION  = "v1.1.0-apha.01"
const BIFROST_VERION = "v1.1.0"

var l sync.RWMutex

type dataTableStruct struct {
	MetaMap			map[string]string //字段类型
	Data 			[]*pluginDriver.PluginDataType
}

type dataStruct struct {
	sync.RWMutex
	Data map[string]*dataTableStruct
}

type fieldStruct struct {
	ToField 		string
	FromMysqlField 	string
	ToFieldType  	string
}

var dataMap map[string]*dataStruct

func init(){
	pluginDriver.Register("mysql",&MyConn{},VERSION,BIFROST_VERION)
	dataMap = make(map[string]*dataStruct,0)
}

type MyConn struct {}

func (MyConn *MyConn) Open(uri string) pluginDriver.ConnFun{
	return newConn(uri)
}

func (MyConn *MyConn) CheckUri(uri string) error{
	c:= newConn(uri)
	if c.err != nil{
		return c.err
	}
	if c.conn == nil{
		c.Close()
		return fmt.Errorf("connect")
	}

	var schemaList []string
	func(){
		defer func() {
			return
		}()
		schemaList = c.conn.GetSchemaList()
	}()
	if len(schemaList) == 0{
		c.Close()
		return fmt.Errorf("schema count is 0 (not in system)")
	}
	return nil
}

func (MyConn *MyConn) GetUriExample() string{
	return "root:root@tcp(127.0.0.1:3306)/test"
}

type Conn struct {
	uri    	string
	status  string
	p		*PluginParam
	conn    *mysqlDB
	err 	error
}

func newConn(uri string) *Conn{
	f := &Conn{
		uri:uri,
	}
	f.Connect()
	return f
}

func (This *Conn) GetConnStatus() string {
	return This.status
}

func (This *Conn) SetConnStatus(status string) {
	This.status = status
}

type PluginParam struct {
	Field 			[]fieldStruct
	BatchSize      	int
	Schema			string
	Table			string
	Datakey			string
	replaceInto		bool  // 记录当前表是否有replace into操作
	PriKey			[]fieldStruct
	toPriKey		string   // toMysql 主键字段
	mysqlPriKey		string  //	对应 from mysql 的主键id
	Data			*dataTableStruct
	fieldCount		int
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
	param.Data = &dataTableStruct{Data:make([]*pluginDriver.PluginDataType,0)}
	param.Datakey = param.Schema+"."+param.Table
	param.toPriKey = param.PriKey[0].ToField
	param.mysqlPriKey = param.PriKey[0].FromMysqlField
	param.fieldCount = len(param.Field)
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
		if err := recover();err != nil{
			This.conn.err = fmt.Errorf(fmt.Sprint(err))
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
	ckFieldsMap := make(map[string]string)
	for _,v:=range fields{
		ckFieldsMap[v.COLUMN_NAME] = v.DATA_TYPE
	}

	for k,v:=range This.p.Field{
		This.p.Field[k].ToFieldType = ckFieldsMap[v.ToField]
	}
}

func (This *Conn) Connect() bool {
	if _,ok:= dataMap[This.uri];!ok{
		dataMap[This.uri] = &dataStruct{
			Data: make(map[string]*dataTableStruct,0),
		}
	}
	This.conn = NewMysqlDBConn(This.uri)
	return true
}

func (This *Conn) ReConnect() bool {
	if This.conn != nil{
		defer func() {
			if err := recover();err !=nil{
				This.conn.err = fmt.Errorf(fmt.Sprint(err))
			}
		}()
		This.conn.Close()
	}
	This.Connect()
	if This.conn.err == nil{
		This.getCktFieldType()
	}
	return  true
}

func (This *Conn) HeartCheck() {
	return
}

func (This *Conn) Close() bool {
	return true
}

func (This *Conn) sendToCacheList(data *pluginDriver.PluginDataType)  (*pluginDriver.PluginBinlog,error){
	var n int
	This.p.Data.Data = append(This.p.Data.Data,data)
	n = len(This.p.Data.Data)
	if This.p.BatchSize <= n{
		return This.Commit()
	}
	return nil,nil
}

func (This *Conn) Insert(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	return This.sendToCacheList(data)
}

func (This *Conn) Update(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	return This.sendToCacheList(data)
}

func (This *Conn) Del(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	return This.sendToCacheList(data)
}

func (This *Conn) Query(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	return nil,nil
}

func (This *Conn) Commit() (b *pluginDriver.PluginBinlog,e error) {
	defer func() {
		if err := recover();err != nil{
			e = fmt.Errorf(string(debug.Stack()))
			This.conn.err = e
		}
	}()
	if This.conn.err != nil {
		This.ReConnect()
	}
	if This.conn.err != nil {
		return nil,This.conn.err
	}
	n := len(This.p.Data.Data)
	if n == 0{
		return nil,nil
	}
	if n > This.p.BatchSize{
		n = This.p.BatchSize
	}
	list := This.p.Data.Data[:n]

	var stmtMap = make(map[string]dbDriver.Stmt,0)

	var getStmt = func(Type string) dbDriver.Stmt{
		if _,ok := stmtMap[Type];ok{
			return stmtMap[Type]
		}
		switch Type {
		case "insert":
			fields := ""
			values := ""
			for _,v:= range This.p.Field{
				if fields == ""{
					fields = v.ToField
					values = "?"
				}else{
					fields += ","+v.ToField
					values += ",?"
				}
			}
			sql := "REPLACE INTO "+This.p.Datakey+" ("+fields+") VALUES ("+values+")"
			stmtMap[Type],This.conn.err = This.conn.conn.Prepare(sql)
			if This.conn.err != nil{
				log.Println("mysql getStmt insert err:",This.conn.err,sql)
			}
			break
		case "delete":
			where := ""
			for _,v:= range This.p.PriKey{
				if where == ""{
					where = v.ToField+"=?"
				}else{
					where += " AND "+v.ToField+"=?"
				}
			}
			stmtMap[Type],This.conn.err = This.conn.conn.Prepare("DELETE FROM "+This.p.Datakey+" WHERE "+where)
			if This.conn.err != nil{
				log.Println("mysql getStmt delete err:",This.conn.err)
			}
			break
		case "update":
			fields := ""
			values := ""
			fields2 := ""
			for _,v:= range This.p.Field{
				if fields == ""{
					fields = v.ToField
					values = "?"
					fields2 = v.ToField+"=?"
				}else{
					fields += ","+v.ToField
					values += ",?"
					fields2 += ","+v.ToField+"=?"
				}
			}
			sql := "INSERT INTO "+This.p.Datakey+" ("+fields+") VALUES ("+values+") ON DUPLICATE KEY UPDATE "+fields2
			stmtMap[Type],This.conn.err = This.conn.conn.Prepare(sql)
			if This.conn.err != nil{
				log.Println("mysql getStmt update err:",This.conn.err,sql)
			}
			break
		}

		return stmtMap[Type]
	}

	This.conn.err = This.conn.Begin()
	if This.conn.err != nil{
		return nil,This.conn.err
	}

	//因为数据是有序写到list里的，里有 update,delete,insert，所以这里我们反向遍历
	// update 转成 insert on update
	// insert 转成 replace into
	// delete 则是 delete 操作
	// 只要是同一条数据，只要有遍历过，后面遍历出来的数据，则不再进行操作

	type opLog struct{
		Data *[]dbDriver.Value
		EventType string
	}

	//用于存储数据库中最后一次操作记录
	opMap := make(map[interface{}]*opLog, 0)

	var checkOpMap = func(key interface{}, EvenType string) bool {
		if _,ok := opMap[key];ok{
			return true
		}
		return false
	}
	//从最后一条数据开始遍历
	var toV dbDriver.Value
	var stmt dbDriver.Stmt
	for i := n - 1; i >= 0; i-- {
		data := list[i]
		switch data.EventType {
		case "update":
			val := make([]dbDriver.Value,This.p.fieldCount*2)
			for i,v:=range This.p.Field{
				toV,This.err = dataTypeTransfer(data.Rows[1][v.FromMysqlField],v.ToField,v.ToFieldType)
				if This.err != nil{
					return nil,This.err
				}
				val[i] = toV
				//第几个字段 + 总字段数量 - 1  算出，on update 所在数组中的位置
				val[i+This.p.fieldCount] = toV
			}

			if checkOpMap(data.Rows[1][This.p.mysqlPriKey], "update") == true {
				continue
			}
			stmt = getStmt("update")
			if stmt == nil{
				goto errLoop
			}
			_,This.conn.err = stmt.Exec(val)
			opMap[data.Rows[1][This.p.mysqlPriKey]] = &opLog{Data:nil,EventType:"update"}
			break
		case "delete":
			where := make([]dbDriver.Value,0)
			for _,v := range This.p.PriKey{
				toV,_ = dataTypeTransfer(data.Rows[0][v.FromMysqlField],v.ToField,v.ToFieldType)
				where = append(where,toV)
			}
			if checkOpMap(data.Rows[0][This.p.mysqlPriKey], "delete") == false {
				stmt = getStmt("delete")
				if stmt == nil{
					goto errLoop
				}
				_,This.conn.err = stmt.Exec(where)
				if This.conn.err != nil{
					goto errLoop
				}
				opMap[data.Rows[0][This.p.mysqlPriKey]] = &opLog{Data:nil,EventType:"delete"}
			}
			break
		case "insert":
			val := make([]dbDriver.Value,0)
			i:=0
			for _,v:=range This.p.Field{
				toV,This.err = dataTypeTransfer(data.Rows[0][v.FromMysqlField],v.ToField,v.ToFieldType)
				if This.err != nil{
					return nil,This.err
				}
				val = append(val,toV)
				i++
			}

			if checkOpMap(data.Rows[0][This.p.mysqlPriKey], "insert") == true {
				continue
			}
			stmt = getStmt("insert")
			if stmt == nil{
				goto errLoop
			}
			_,This.conn.err = stmt.Exec(val)
			if This.conn.err != nil{
				goto errLoop
			}
			opMap[data.Rows[0][This.p.mysqlPriKey]] = &opLog{Data:&val,EventType:"insert"}
			break
		}

	}

	errLoop:
		if This.conn.err != nil{
			This.err = This.conn.err
		}
		if This.err != nil{
			This.conn.Rollback()
			log.Println("This.err",This.err)
			return nil,This.err
		}

	err2 := This.conn.Commit()
	if err2 != nil{
		This.conn.err = err2
		return nil,This.conn.err
	}

	if len(This.p.Data.Data) <= int(This.p.BatchSize){
		This.p.Data.Data = make([]*pluginDriver.PluginDataType,0)
	}else{
		This.p.Data.Data = This.p.Data.Data[n+1:]
	}

	return &pluginDriver.PluginBinlog{list[n-1].BinlogFileNum,list[n-1].BinlogPosition}, nil
}

func dataTypeTransfer(data interface{},fieldName string,toDataType string) (v dbDriver.Value,e error) {
	defer func() {
		if err := recover();err != nil{
			e = fmt.Errorf(fieldName+" "+fmt.Sprint(err))
		}
	}()
	if data == nil{
		v = nil
		return
	}
	switch toDataType {
	case "bool":
		switch data.(type) {
		case bool:
			if data.(bool) == true{
				v = "1"
			}else{
				v = "0"
			}
			break
		default:
			if fmt.Sprint(data) != ""{
				v = "1"
			}else{
				v = "0"
			}
			break
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
		case []string:
			v = strings.Replace(strings.Trim(fmt.Sprint(data), "[]"), " ", ",", -1)
			break
		default:
			v = fmt.Sprint(data)
			break
		}
		break
	default:
		v = fmt.Sprint(data)
		break
	}
	return
}
