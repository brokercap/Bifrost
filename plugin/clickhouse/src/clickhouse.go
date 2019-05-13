package src

import (
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"sync"
	"encoding/json"
	"fmt"
	dbDriver "database/sql/driver"
	"log"
	"strconv"
	"runtime/debug"
	"strings"
	"time"
)


const VERSION  = "v1.1.0-beta.01"
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
	CK 		string
	MySQL 	string
	CkType  string
}

var dataMap map[string]*dataStruct

func init(){
	pluginDriver.Register("clickhouse",&MyConn{},VERSION,BIFROST_VERION)
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
	return "tcp://127.0.0.1:9000?username=&compress=true&debug=true"
}

type Conn struct {
	uri    	string
	status  string
	p		*PluginParam
	conn    *clickhouseDB
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
	BatchSize      int
	CkSchema		string
	CkTable			string
	ckDatakey		string
	replaceInto		bool  // 记录当前表是否有replace into操作
	PriKey			[]fieldStruct
	ckPriKey		string   // ck 主键字段
	mysqlPriKey		string  //ck对应 mysql 的主键id
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
	param.ckDatakey = param.CkSchema+"."+param.CkTable
	param.ckPriKey = param.PriKey[0].CK
	param.mysqlPriKey = param.PriKey[0].MySQL
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

	ckFields := This.conn.GetTableFields(This.p.ckDatakey)
	if This.conn.err != nil{
		This.err = This.conn.err
		return
	}
	if len(ckFields) == 0{
		return
}
	ckFieldsMap := make(map[string]string)
	for _,v:=range ckFields{
		ckFieldsMap[v.Name] = v.Type
	}

	for k,v:=range This.p.Field{
		This.p.Field[k].CkType = ckFieldsMap[v.CK]
	}
}

func (This *Conn) Connect() bool {
	if _,ok:= dataMap[This.uri];!ok{
		dataMap[This.uri] = &dataStruct{
			Data: make(map[string]*dataTableStruct,0),
		}
	}
	This.conn = NewClickHouseDBConn(This.uri)
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
	l.RLock()
	t := dataMap[This.uri]
	t.Lock()
	if _,ok := t.Data[This.p.ckDatakey];!ok{
		t.Data[This.p.ckDatakey] = &dataTableStruct{
			MetaMap:make(map[string]string,0),
			Data:make([]*pluginDriver.PluginDataType,0),
		}
	}
	t.Data[This.p.ckDatakey].Data = append(t.Data[This.p.ckDatakey].Data,data)
	n = len(t.Data[This.p.ckDatakey].Data)
	t.Unlock()
	l.RUnlock()

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
	t := dataMap[This.uri]
	t.Lock()
	defer t.Unlock()
	if _,ok := t.Data[This.p.ckDatakey];!ok{
		return nil,nil
	}
	n := len(t.Data[This.p.ckDatakey].Data)
	if n == 0{
		return nil,nil
	}
	if n > This.p.BatchSize{
		n = This.p.BatchSize
	}
	list := t.Data[This.p.ckDatakey].Data[:n]

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
					fields = v.CK
					values = "?"
				}else{
					fields += ","+v.CK
					values += ",?"
				}
			}
			sql := "INSERT INTO "+This.p.ckDatakey+" ("+fields+") VALUES ("+values+")"
			stmtMap[Type],This.conn.err = This.conn.conn.Prepare(sql)
			if This.conn.err != nil{
				log.Println("clickhouse getStmt insert err:",This.conn.err)
			}
			break
		case "delete":
			where := ""
			for _,v:= range This.p.PriKey{
				if where == ""{
					where = v.CK+"=?"
				}else{
					where += " AND "+v.CK+"=?"
				}
			}
			stmtMap[Type],This.conn.err = This.conn.conn.Prepare("ALTER TABLE "+This.p.ckDatakey+" DELETE WHERE "+where)
			if This.conn.err != nil{
				log.Println("clickhouse getStmt delete err:",This.conn.err)
			}
			break
		}
		return stmtMap[Type]
	}

	_,This.conn.err = This.conn.conn.Begin()
	if This.conn.err != nil{
		return nil,This.conn.err
	}

	//因为数据是有序写到list里的，里有 update,delete,insert，所以这里我们反向遍历
	//假如update之前，则说明，后面遍历的同一条数据都不需要再更新了
	//有一种比较糟糕的情况就是在源端replace into 操作，这个操作是先delete再insert操作，
	//所以这种情况。假如自动发现了，则应该是再删除一次,然后再最后再重新执行一次insert最后的记录

	type opLog struct{
		Data *[]dbDriver.Value
		EventType string
	}
	//在最后是Insert但 之前又有delete操作的情况下,把最后insert的数据,存储在这个list中,在代码最后再执行一次insert
	needDoubleInsertOp := make([][]dbDriver.Value,0)

	//用于存储数据库中最后一次操作记录
	opMap := make(map[interface{}]*opLog, 0)

	var checkOpMap = func(key interface{}, EvenType string) bool {
		if opMap[key] == nil{
			return false
		}
		//假如insert了,但是又delete,则说明 db中有先delete再insert的操作,这个时候需要将insert的内容存储起来,最后再insert一次
		if opMap[key].EventType != "update" && EvenType == "delete" {
			opMap[key].EventType = "delete"
			needDoubleInsertOp = append(needDoubleInsertOp,*opMap[key].Data)
			return false
		}
		return true
	}
	//从最后一条数据开始遍历
	var toV interface{}
	var stmt dbDriver.Stmt
	for i := n - 1; i >= 0; i-- {
		data := list[i]
		switch data.EventType {
		case "update":
			val := make([]dbDriver.Value,0)
			for _,v:=range This.p.Field{
				toV,This.err = ckDataTypeTransfer(data.Rows[1][v.MySQL],v.CK,v.CkType)
				if This.err != nil{
					return nil,This.err
				}
				val = append(val,toV)
			}

			if checkOpMap(data.Rows[1][This.p.mysqlPriKey], "update") == true {
				continue
			}

			where := make([]dbDriver.Value,1)
			where[0] = data.Rows[0][This.p.mysqlPriKey]
			stmt = getStmt("delete")
			if stmt == nil{
				goto errLoop
			}
			_,This.err = stmt.Exec(where)
			stmt = getStmt("insert")
			if stmt == nil{
				goto errLoop
			}
			_,This.conn.err = stmt.Exec(val)
			opMap[data.Rows[1][This.p.mysqlPriKey]] = &opLog{Data:nil,EventType:"update"}
			break
		case "delete":
			where := make([]dbDriver.Value,1)
			where[0] = data.Rows[0][This.p.mysqlPriKey]
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
			for _,v:=range This.p.Field{
				toV,This.err = ckDataTypeTransfer(data.Rows[0][v.MySQL],v.CK,v.CkType)
				if This.err != nil{
					return nil,This.err
				}
				val = append(val,toV)
			}

			if checkOpMap(data.Rows[0][This.p.mysqlPriKey], "insert") == true {
				continue
			}

			//log.Println("insert:",val)
			stmt = getStmt("insert")
			if stmt == nil{
				goto errLoop
			}

			_,This.conn.err = stmt.Exec(val)
			if This.conn.err != nil{
				This.conn.conn.Rollback()
				return nil,This.conn.err
			}
			opMap[data.Rows[0][This.p.mysqlPriKey]] = &opLog{Data:&val,EventType:"insert"}
			break
		}

	}

	errLoop:
		if This.err != nil{
			This.conn.conn.Rollback()
			return nil,This.err
		}
	for _,val := range needDoubleInsertOp{
		getStmt("insert").Exec(val)
	}

	err2 := This.conn.conn.Commit()
	if err2 != nil{
		This.conn.err = err2
		return nil,This.conn.err
	}

	if len(t.Data[This.p.ckDatakey].Data) <= int(This.p.BatchSize){
		t.Data[This.p.ckDatakey].Data = make([]*pluginDriver.PluginDataType,0)
	}else{
		t.Data[This.p.ckDatakey].Data = t.Data[This.p.ckDatakey].Data[n+1:]
	}

	return &pluginDriver.PluginBinlog{list[n-1].BinlogFileNum,list[n-1].BinlogPosition}, nil
}

func ckDataTypeTransfer(data interface{},fieldName string,toDataType string) (v interface{},e error) {
	defer func() {
		if err := recover();err != nil{
			e = fmt.Errorf(fieldName+" "+fmt.Sprint(err))
		}
	}()
	switch toDataType {
	case "Date":
		if data == nil{
			v = int16(0)
		}else{
			if data.(string) == "0000-00-00"{
				v = int16(0)
			}else{
				v = data
			}
		}
		break
	case "DateTime":
		if data == nil{
			v = int32(0)
		}else{
			if data.(string) == "0000-00-00 00:00:00"{
				v = int32(0)
			}else{
				//log.Println("DateTime:",time.Now().Format("2006-01-02 15:04:05"))
				loc, _ := time.LoadLocation("Local")                            //重要：获取时区
				theTime, _ := time.ParseInLocation("2006-01-02 15:04:05", data.(string), loc) //使用模板在对应时区转化为time.time类型
				v = theTime.Unix()
			}
		}
		break
	case "String","Enum8","Enum16","Enum","UUID":
		if data == nil{
			v = ""
			break
		}
		switch data.(type) {
		case []string:
			v = strings.Replace(strings.Trim(fmt.Sprint(data), "[]"), " ", ",", -1)
			break
		default:
			v = fmt.Sprint(data)
			break
		}
		break
	case "Int8":
		if data == nil{
			v = int(0)
			break
		}
		switch data.(type) {
		case bool:
			if data.(bool) == true{
				v = int8(1)
			}else{
				v = int8(0)
			}
			break
		default:
			v = data.(int8)
			break
		}
		break
	case "UInt8":
		if data == nil{
			v = uint(0)
			break
		}
		v = data.(uint8)
		break
	case "Int16":
		if data == nil{
			v = int16(0)
			break
		}
		//mysql year 类型对应go int类型，但是ck里可能是Int16
		switch data.(type) {
		case string:
			s1,_ := strconv.Atoi(data.(string))
			v = int16(s1)
			break
		case int:
			v = int16(data.(int))
			break
		default:
			v = data.(int16)
			break
		}
		break
	case "UInt16":
		if data == nil{
			v = uint16(0)
			break
		}
		v = data.(uint16)
		break
	case "Int32":
		if data == nil{
			v = int32(0)
			break
		}
		v = data.(int32)
		break
	case "UInt32":
		if data == nil{
			v = uint32(0)
			break
		}
		v = data.(uint32)
		break
	case "Int64":
		if data == nil{
			v = int64(0)
			break
		}
		v = data.(int64)
		break
	case "UInt64":
		if data == nil{
			v = uint64(0)
			break
		}
		v = data.(uint64)
		break
	case "Float64":
		if data == nil{
			v = float64(0.00)
			break
		}
		// 有可能是decimal 类型，binlog解析出来decimal 对应go string类型
		switch data.(type) {
		case string:
			s1,_ := strconv.ParseFloat(data.(string), 64)
			v = s1
			break
		case float32:
			v = float64(data.(float32))
			break
		default:
			v = data.(float64)
			break
		}
		break
	case "Float32":
		if data == nil{
			v = float32(0.00)
			break
		}
		switch data.(type) {
		case string:
			s1,_ := strconv.ParseFloat(data.(string), 32)
			v = s1
			break
		default:
			v = data.(float32)
			break
		}
		break
	default:
		v = fmt.Sprint(data)
		break
	}
	return
}
