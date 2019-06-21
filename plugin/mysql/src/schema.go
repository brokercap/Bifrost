package src

import (
	"database/sql/driver"
	"github.com/brokercap/Bifrost/manager/xgo"
	"net/http"
	"encoding/json"
	pluginStorage "github.com/brokercap/Bifrost/plugin/storage"
	"github.com/brokercap/Bifrost/Bristol/mysql"
	"log"
	"fmt"
)

func init()  {
	xgo.AddRoute("/bifrost/mysql/tableinfo",getmysqlTableFields)
	xgo.AddRoute("/bifrost/mysql/schemalist",getmysqlSchemaList)
	xgo.AddRoute("/bifrost/mysql/tablelist",getmysqlSchemaTableList)
}

func getmysqlSchemaList(w http.ResponseWriter,req *http.Request)  {
	req.ParseForm()
	ToServerKey := req.Form.Get("toserverkey")
	toServerInfo := pluginStorage.GetToServerInfo(ToServerKey)
	if toServerInfo == nil{
		w.Write([]byte(ToServerKey+" no found"))
		return
	}
	c := NewMysqlDBConn(toServerInfo.ConnUri)
	defer c.Close()
	m := c.GetSchemaList()
	b,_:=json.Marshal(m)
	w.Write(b)
	return
}

func getmysqlSchemaTableList(w http.ResponseWriter,req *http.Request)  {
	req.ParseForm()
	ToServerKey := req.Form.Get("toserverkey")
	toServerInfo := pluginStorage.GetToServerInfo(ToServerKey)
	if toServerInfo == nil{
		w.Write([]byte(ToServerKey+" no found"))
		return
	}
	schema := req.Form.Get("schema")
	c := NewMysqlDBConn(toServerInfo.ConnUri)
	defer c.Close()
	m := c.GetSchemaTableList(schema)
	b,_:=json.Marshal(m)
	w.Write(b)
	return
}


func getmysqlTableFields(w http.ResponseWriter,req *http.Request)  {
	req.ParseForm()
	ToServerKey := req.Form.Get("toserverkey")
	toServerInfo := pluginStorage.GetToServerInfo(ToServerKey)
	if toServerInfo == nil{
		w.Write([]byte(ToServerKey+" no found"))
		return
	}
	schema := req.Form.Get("schema")
	TableName := req.Form.Get("table_name")
	c := NewMysqlDBConn(toServerInfo.ConnUri)
	defer c.Close()
	m := c.GetTableFields(schema,TableName)
	b,_:=json.Marshal(m)
	w.Write(b)
	return
}

func NewMysqlDBConn(uri string) *mysqlDB {
	c := &mysqlDB{
		uri:uri,
	}
	c.Open()
	return c
}

type mysqlDB struct {
	uri 	string
	conn 	mysql.MysqlConnection
	err 	error
}

func(This *mysqlDB) Open() (b bool){
	defer func() {
		if err:=recover();err!=nil{
			This.err = fmt.Errorf(fmt.Sprint(err))
			b = false
		}
	}()
	This.conn = mysql.NewConnect(This.uri)
	return true
}

func(This *mysqlDB) Close() bool{
	defer func() {
		if err := recover();err != nil{
			log.Println("mysqlDB close err:",err)
		}
	}()
	if This.conn != nil{
		This.conn.Close()
	}
	return true
}

func (This *mysqlDB) GetSchemaList() (data []string) {
	stmt, _ := This.conn.Prepare("SHOW DATABASES")
	defer stmt.Close()
	rows, err := stmt.Query([]driver.Value{})
	if err != nil {
		This.err = err
		return
	}
	defer rows.Close()
	filterMap := make(map[string]bool,3)
	filterMap["performance_schema"] = true
	filterMap["information_schema"] = true
	filterMap["mysql"] 				= true

	for {
		dest := make([]driver.Value, 1, 1)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		var DataBase string
		DataBase 		= string(dest[0].([]byte))
		if _,ok := filterMap[DataBase];ok{
			continue
		}
		data = append(data,DataBase)
	}
	return
}


func (This *mysqlDB) GetSchemaTableList(schema string) (data []string) {
	if schema == ""{
		return
	}
	sql := "SELECT TABLE_NAME FROM `information_schema`.`TABLES` WHERE TABLE_SCHEMA = ?"
	stmt, _ := This.conn.Prepare(sql)
	defer stmt.Close()
	p := make([]driver.Value, 0)
	p = append(p,schema)
	rows, err := stmt.Query(p)
	if err != nil {
		This.err = err
		return
	}
	defer rows.Close()

	for {
		dest := make([]driver.Value, 1, 1)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		data = append(data,string(dest[0].([]byte)))
	}
	return
}


type TableStruct struct {
	COLUMN_NAME 		string
	COLUMN_DEFAULT 		string
	IS_NULLABLE 		string
	COLUMN_TYPE			string
	COLUMN_KEY 			string
	EXTRA 				string
	COLUMN_COMMENT 		string
	DATA_TYPE			string
	NUMERIC_PRECISION	string
	NUMERIC_SCALE		string
}


func (This *mysqlDB) GetTableFields(schema,table string) (data []TableStruct) {
	FieldList := make([]TableStruct,0)
	sql := "SELECT `COLUMN_NAME`,`COLUMN_DEFAULT`,`IS_NULLABLE`,`COLUMN_TYPE`,`COLUMN_KEY`,`EXTRA`,`COLUMN_COMMENT`,`DATA_TYPE`,`NUMERIC_PRECISION`,`NUMERIC_SCALE` FROM `information_schema`.`columns` WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "

	stmt,err := This.conn.Prepare(sql)
	if err !=nil{
		log.Println(err)
		return FieldList
	}
	defer stmt.Close()
	p := make([]driver.Value, 0)
	p = append(p,schema)
	p = append(p,table)
	rows, err := stmt.Query(p)
	defer rows.Close()
	if err != nil {
		log.Printf("%v\n", err)
		return FieldList
	}

	for {
		dest := make([]driver.Value, 10, 10)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		var COLUMN_NAME string
		var COLUMN_DEFAULT string
		var IS_NULLABLE string
		var COLUMN_TYPE string
		var COLUMN_KEY string
		var EXTRA string
		var COLUMN_COMMENT string
		var DATA_TYPE string
		var NUMERIC_PRECISION string
		var NUMERIC_SCALE string

		COLUMN_NAME 		= string(dest[0].([]byte))
		COLUMN_DEFAULT 		= string(dest[1].([]byte))
		IS_NULLABLE 		= string(dest[2].([]byte))
		COLUMN_TYPE 		= string(dest[3].([]byte))
		COLUMN_KEY 			= string(dest[4].([]byte))
		EXTRA 				= string(dest[5].([]byte))
		COLUMN_COMMENT 		= string(dest[6].([]byte))
		DATA_TYPE 			= string(dest[7].([]byte))
		NUMERIC_PRECISION 	= string(dest[8].([]byte))
		NUMERIC_SCALE 		= string(dest[9].([]byte))
		if COLUMN_TYPE=="tinyint(1)"{
			DATA_TYPE = "bool"
		}

		FieldList = append(FieldList,TableStruct{
			COLUMN_NAME:	COLUMN_NAME,
			COLUMN_DEFAULT:	COLUMN_DEFAULT,
			IS_NULLABLE:	IS_NULLABLE,
			COLUMN_TYPE:	COLUMN_TYPE,
			COLUMN_KEY:		COLUMN_KEY,
			EXTRA:			EXTRA,
			COLUMN_COMMENT:	COLUMN_COMMENT,
			DATA_TYPE:		DATA_TYPE,
			NUMERIC_PRECISION:NUMERIC_PRECISION,
			NUMERIC_SCALE:	NUMERIC_SCALE,
		})
	}
	return FieldList
}


func (This *mysqlDB) Begin() error {
	_,err := This.conn.Exec("BEGIN",make([]driver.Value,0))
	return  err
}

func (This *mysqlDB) Commit() error {
	_,err := This.conn.Exec("COMMIT",make([]driver.Value,0))
	return err
}

func (This *mysqlDB) Rollback() error {
	_,err := This.conn.Exec("ROLLBACK",make([]driver.Value,0))
	return err
}