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


const VERSION  = "v1.1.0-rc.06"
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


func init(){
	pluginDriver.Register("clickhouse",&MyConn{},VERSION,BIFROST_VERION)
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
	conn    *ClickhouseDB
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

type SyncType string

const (
	FILTER_DEFEALT SyncType = ""
	FILTER_NORMAL SyncType = "Normal"
	FILTER_INSERT_ALL SyncType = "insertAll"
)

type PluginParam struct {
	Field 			[]fieldStruct
	BatchSize      	int
	CkSchema		string
	CkTable			string
	ckDatakey		string
	replaceInto		bool  // 记录当前表是否有replace into操作
	PriKey			[]fieldStruct
	ckPriKey		string   // ck 主键字段
	mysqlPriKey		string  //ck对应 mysql 的主键id
	Data			*dataTableStruct
	SyncType		SyncType
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
	param.Data = &dataTableStruct{Data:make([]*pluginDriver.PluginDataType,0)}
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
	if This.conn == nil {
		This.conn = NewClickHouseDBConn(This.uri)
	}
	return true
}

func (This *Conn) ReConnect() bool {
	This.Close()
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
	if This.conn != nil{
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

func (This *Conn) getStmt(Type string) dbDriver.Stmt {
	var stmt dbDriver.Stmt
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
		stmt,This.conn.err = This.conn.conn.Prepare(sql)
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
		stmt,This.conn.err = This.conn.conn.Prepare("ALTER TABLE "+This.p.ckDatakey+" DELETE WHERE "+where)
		if This.conn.err != nil{
			log.Println("clickhouse getStmt delete err:",This.conn.err)
		}
		break
	}

	if This.conn.err != nil{
		return nil
	}
	return stmt
}

func (This *Conn) getMySQLData(data *pluginDriver.PluginDataType,index int,key string) interface{} {
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

func (This *Conn) getMySQLData2(data map[string]interface{},key string) interface{} {
	if _,ok := data[key];ok {
		return data[key]
	}
	if key == "{$Timestamp}"{
		return time.Now().Unix()
	}
	return  key
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

	deleteDataMap := make(map[interface{}]map[string]interface{},0)
	insertDataMap := make(map[interface{}]map[string]interface{},0)

	var ok bool

	var normalFun = func(v *pluginDriver.PluginDataType) {
		switch v.EventType {
		case "insert":
			for _,row := range v.Rows{
				key := row[This.p.mysqlPriKey]
				if _,ok=deleteDataMap[key];!ok {
					if _, ok = insertDataMap[key]; !ok {
						insertDataMap[key] = row
					}
				}
			}
			break
		case "update":
			for k := len(v.Rows)-1; k >= 0;k--{
				row := v.Rows[k]
				key := row[This.p.mysqlPriKey]
				if k%2 == 0{
					if _,ok:=deleteDataMap[key];!ok{
						deleteDataMap[key] = row
					}
				}else{
					if _,ok=deleteDataMap[key];!ok {
						if _, ok = insertDataMap[key]; !ok {
							insertDataMap[key] = row
						}
					}
				}
			}
			break
		case "delete":
			for _,row := range v.Rows{
				key := row[This.p.mysqlPriKey]
				if _,ok:=deleteDataMap[key];!ok{
					deleteDataMap[key] = row
				}
			}
			break
		default:
			break
		}
	}

	var stmt dbDriver.Stmt

	if This.p.SyncType == FILTER_INSERT_ALL{
		_, This.conn.err = This.conn.conn.Begin()
		if This.conn.err != nil {
			return nil, This.conn.err
		}
		var toV interface{}
		stmt = This.getStmt("insert")
		for i := n - 1; i >= 0; i-- {
			vData := list[i]
			val := make([]dbDriver.Value, 0)
			l := len(vData.Rows)
			switch vData.EventType {
			case "insert","delete":
				for k := 0; k < l ;k++{
					for _, v := range This.p.Field {
						toV, This.err = CkDataTypeTransfer(This.getMySQLData(vData,k,v.MySQL), v.CK, v.CkType)
						if This.err != nil {
							stmt.Close()
							goto errLoop
						}
						val = append(val, toV)
					}
				}
				break
			case "update":
				for k := 0; k < l ;k++{
					if k%2 != 0 {
						for _, v := range This.p.Field {
							toV, This.err = CkDataTypeTransfer(This.getMySQLData(vData,k,v.MySQL), v.CK, v.CkType)
							if This.err != nil {
								stmt.Close()
								goto errLoop
							}
							val = append(val, toV)
						}
					}
				}
				break
			default:
				break
			}
			_, This.err = stmt.Exec(val)
			if This.err != nil {
				stmt.Close()
				goto errLoop
			}
		}
	}else{
		for i := n - 1; i >= 0; i-- {
			v := list[i]
			normalFun(v)
		}
		_, This.conn.err = This.conn.conn.Begin()
		if This.conn.err != nil {
			return nil, This.conn.err
		}

		if len(deleteDataMap) > 0 {
			stmt = This.getStmt("delete")
			if stmt == nil {
				goto errLoop
			}
			for _, v := range deleteDataMap {
				where := make([]dbDriver.Value, 1)
				where[0] = v[This.p.mysqlPriKey]
				_, This.err = stmt.Exec(where)
				if This.err != nil {
					stmt.Close()
					goto errLoop
				}
			}
			stmt.Close()
		}

		if len(insertDataMap) > 0 {
			stmt = This.getStmt("insert")
			if stmt == nil {
				goto errLoop
			}
			var toV interface{}
			for _, dataMap := range insertDataMap {
				val := make([]dbDriver.Value, 0)
				for _, v := range This.p.Field {
					toV, This.err = CkDataTypeTransfer(This.getMySQLData2(dataMap,v.MySQL), v.CK, v.CkType)
					if This.err != nil {
						stmt.Close()
						goto errLoop
					}
					val = append(val, toV)
				}
				_, This.err = stmt.Exec(val)
				if This.err != nil {
					stmt.Close()
					goto errLoop
				}
			}
			stmt.Close()
		}
	}

	errLoop:
		if This.conn.err != nil{
			This.err = This.conn.err
			This.conn.conn.Rollback()
			return nil,This.conn.err
		}
		if This.err != nil{
			This.conn.conn.Rollback()
			return nil,This.err
		}

	err2 := This.conn.conn.Commit()
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

func AllTypeToInt64(s interface{}) (int64,error) {
	t := strings.Trim(fmt.Sprint(s)," ")
	t = strings.Trim(t,"　")
	i64,err := strconv.ParseInt(t,10,64)
	if err != nil {
		return 0,nil
	}
	return i64,nil
}

func AllTypeToUInt64(s interface{}) (uint64,error) {
	t := strings.Trim(fmt.Sprint(s)," ")
	t = strings.Trim(t,"　")
	ui64,err := strconv.ParseUint(t,10,64)
	if err != nil {
		return 0,nil
	}
	return ui64,nil
}

func CkDataTypeTransfer(data interface{},fieldName string,toDataType string) (v interface{},e error) {
	defer func() {
		if err := recover();err != nil{
			e = fmt.Errorf(fieldName+" "+fmt.Sprint(err))
		}
	}()
	switch toDataType {
	case "Date","Nullable(Date)":
		if data == nil{
			v = int16(0)
			break
		}
		switch data.(type) {
		case int16:
			v = data
			break
		case string:
			if data.(string) == "0000-00-00"{
				v = int16(0)
			}else{
				v = data
			}
			break
		default:
			i64,err := AllTypeToInt64(data)
			if err != nil{
				return 0,err
			}
			if i64 <= 32767 && i64 >= -32768{
				v = int16(i64)
			}else{
				v = int16(0)
			}
			break
		}
		break
	case "DateTime","Nullable(DateTime)":
		if data == nil{
			v = int32(0)
			break
		}
		switch data.(type) {
		case int32:
			v = data
			break
		case string:
			if data.(string) == "0000-00-00 00:00:00"{
				v = int32(0)
			}else{
				loc, _ := time.LoadLocation("Local")                            //重要：获取时区
				theTime, _ := time.ParseInLocation("2006-01-02 15:04:05", data.(string), loc) //使用模板在对应时区转化为time.time类型
				v = theTime.Unix()
			}
			break
		default:
			i64,err := AllTypeToInt64(data)
			if err != nil{
				return 0,err
			}
			if i64 <= 2147483647 && i64 >=  -2147483648{
				v = int32(i64)
			}else{
				v = int32(0)
			}
			break
		}
		break
	case "String","Enum8","Enum16","Enum","UUID","Nullable(String)","Nullable(Enum8)","Nullable(Enum16)","Nullable(Enum)","Nullable(UUID)":
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
	case "Int8","Nullable(Int8)":
		if data == nil{
			v = int8(0)
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
		case int8:
			v = data
			break
		default:
			i64,err := AllTypeToInt64(data)
			if err != nil{
				return 0,err
			}
			if i64 <= 127 && i64 >=  -128{
				v = int8(i64)
			}else{
				v = int8(0)
			}
			break
		}
		break
	case "UInt8","Nullable(UInt8)":
		if data == nil{
			v = uint8(0)
			break
		}
		switch data.(type) {
		case uint8:
			v = data
			break
		default:
			i64,err := AllTypeToUInt64(data)
			if err != nil{
				return 0,err
			}
			if i64 <= 255{
				v = uint8(i64)
			}else {
				v = uint8(0)
			}
			break
		}
		break
	case "Int16","Nullable(Int16)":
		if data == nil{
			v = int16(0)
			break
		}
		//mysql year 类型对应go int类型，但是ck里可能是Int16
		switch data.(type) {
		case int16:
			v = data
			break
		default:
			i64,err := AllTypeToInt64(data)
			if err != nil{
				return 0,err
			}
			if i64 <= 32767 && i64 >= -32768{
				v = int16(i64)
			}else{
				v = int16(0)
			}
			break
		}
		break
	case "UInt16","Nullable(UInt16)":
		if data == nil{
			v = uint16(0)
			break
		}
		switch data.(type) {
		case uint16:
			v = data
			break
		default:
			i64,err := AllTypeToUInt64(data)
			if err != nil{
				return 0,err
			}
			if i64 <= 65535{
				v = uint16(i64)
			}else {
				v = uint16(0)
			}
			break
		}
		break
	case "Int32","Nullable(Int32)":
		if data == nil{
			v = int32(0)
			break
		}
		switch data.(type) {
		case int32:
			v = data
			break
		default:
			i64,err := AllTypeToInt64(data)
			if err != nil{
				return 0,err
			}
			if i64 <= 2147483647 && i64 >=  -2147483648{
				v = int32(i64)
			}else{
				v = int32(0)
			}
			break
		}
		break
	case "UInt32","Nullable(UInt32)":
		if data == nil{
			v = uint32(0)
			break
		}
		switch data.(type) {
		case uint32:
			v = data
			break
		default:
			i64,err := AllTypeToUInt64(data)
			if err != nil{
				return 0,err
			}
			if i64 <= 4294967295{
				v = uint32(i64)
			}else{
				v = uint32(0)
			}
			break
		}
		break
	case "Int64","Nullable(Int64)":
		if data == nil{
			v = int64(0)
			break
		}
		switch data.(type) {
		case int64:
			v = data
			break
		default:
			i64,err := AllTypeToInt64(data)
			if err != nil{
				return 0,err
			}
			v = i64
			break
		}
		break
	case "UInt64","Nullable(UInt64)":
		if data == nil{
			v = uint64(0)
			break
		}
		switch data.(type) {
		case uint64:
			v = data
			break
		default:
			i64,err := AllTypeToUInt64(data)
			if err != nil{
				return 0,err
			}
			v = i64
			break
		}
		break
	case "Float64","Nullable(Float64)":
		if data == nil{
			v = float64(0.00)
			break
		}
		// 有可能是decimal 类型，binlog解析出来decimal 对应go string类型
		switch data.(type) {
		case float64:
			v = data
			break
		case float32:
			v = float64(data.(float32))
			break
		default:
			v = interfaceToFloat64(data)
			break
		}
		break
	case "Float32","Float","Nullable(Float32)","Nullable(Float)":
		if data == nil{
			v = float32(0.00)
			break
		}

		switch data.(type) {
		case float32:
			v = data
			break
		case float64:
			v = float32(data.(float64))
			break
		default:
			v = float32(interfaceToFloat64(data))
			break
		}
		break
	default:
		//Decimal
		if toDataType[0:3] == "Dec" || ( toDataType[0:3] == "Nul" && strings.Contains(toDataType,"Decimal") ) {
			v = interfaceToFloat64(data)
		}else{
			v = fmt.Sprint(data)
		}
		break
	}
	return
}

func interfaceToFloat64(data interface{}) float64  {
	t := strings.Trim(fmt.Sprint(data)," ")
	t = strings.Trim(t,"　")
	f1,err := strconv.ParseFloat(t, 64)
	if err != nil{
		return float64(0.00)
	}
	return f1
}