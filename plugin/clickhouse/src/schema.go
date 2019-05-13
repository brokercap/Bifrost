package src

import (
	"database/sql/driver"
	"github.com/kshvakov/clickhouse"
	"github.com/brokercap/Bifrost/manager/xgo"
	"net/http"
	"encoding/json"
	pluginStorage "github.com/brokercap/Bifrost/plugin/storage"

	"log"
)

func init()  {
	xgo.AddRoute("/bifrost/clickhouse/tableinfo",getClickHouseTableFields)
	xgo.AddRoute("/bifrost/clickhouse/schemalist",getClickHouseSchemaList)
	xgo.AddRoute("/bifrost/clickhouse/tablelist",getClickHouseSchemaTableList)
}

type ckFieldStruct struct {
	Name 				string
	Type 				string
	DefaultType 		string
	DefaultExpression 	string
}

func getClickHouseSchemaList(w http.ResponseWriter,req *http.Request)  {
	req.ParseForm()
	ToServerKey := req.Form.Get("toserverkey")
	toServerInfo := pluginStorage.GetToServerInfo(ToServerKey)
	if toServerInfo == nil{
		w.Write([]byte(ToServerKey+" no found"))
		return
	}
	c := NewClickHouseDBConn(toServerInfo.ConnUri)
	defer c.Close()
	m := c.GetSchemaList()
	b,_:=json.Marshal(m)
	w.Write(b)
	return
}

func getClickHouseSchemaTableList(w http.ResponseWriter,req *http.Request)  {
	req.ParseForm()
	ToServerKey := req.Form.Get("toserverkey")
	toServerInfo := pluginStorage.GetToServerInfo(ToServerKey)
	if toServerInfo == nil{
		w.Write([]byte(ToServerKey+" no found"))
		return
	}
	schema := req.Form.Get("schema")
	c := NewClickHouseDBConn(toServerInfo.ConnUri)
	defer c.Close()
	m := c.GetSchemaTableList(schema)
	b,_:=json.Marshal(m)
	w.Write(b)
	return
}


func getClickHouseTableFields(w http.ResponseWriter,req *http.Request)  {
	req.ParseForm()
	ToServerKey := req.Form.Get("toserverkey")
	toServerInfo := pluginStorage.GetToServerInfo(ToServerKey)
	if toServerInfo == nil{
		w.Write([]byte(ToServerKey+" no found"))
		return
	}
	schema := req.Form.Get("schema")
	TableName := req.Form.Get("table_name")
	c := NewClickHouseDBConn(toServerInfo.ConnUri)
	defer c.Close()
	m := c.GetTableFields(schema+"."+TableName)
	b,_:=json.Marshal(m)
	w.Write(b)
	return
}

func NewClickHouseDBConn(uri string) *clickhouseDB {
	c := &clickhouseDB{
		uri:uri,
	}
	c.Open()
	return c
}

type clickhouseDB struct {
	uri 	string
	conn 	clickhouse.Clickhouse
	err 	error
}

func(This *clickhouseDB) Open() bool{
	This.conn, This.err = clickhouse.OpenDirect(This.uri)
	return true
}

func(This *clickhouseDB) Close() bool{
	defer func() {
		if err := recover();err != nil{
			log.Println("clickhouseDB close err:",err)
		}
	}()
	if This.conn != nil{
		This.conn.Close()
	}
	return true
}

func (This *clickhouseDB) GetSchemaList() (data []string) {
	This.conn.Begin()
	stmt, _ := This.conn.Prepare("SHOW DATABASES")
	rows, err := stmt.Query([]driver.Value{})
	if err != nil {
		This.err = err
		return
	}
	defer rows.Close()
	row := make([]driver.Value, 1)

	for rows.Next(row) == nil {
		//过滤system库
		if row[0].(string) == "system"{
			continue
		}
		data = append(data,row[0].(string))
	}
	This.conn.Commit()
	return
}


func (This *clickhouseDB) GetSchemaTableList(schema string) (data []string) {
	if schema == ""{
		return
	}

	This.conn.Begin()
	stmt, _ := This.conn.Prepare("select name from system.tables where database = '"+schema+"'")
	rows, err := stmt.Query([]driver.Value{})
	if err != nil {
		This.err = err
		return
	}
	defer rows.Close()
	row := make([]driver.Value, 1)

	for rows.Next(row) == nil {
		data = append(data,row[0].(string))
	}
	This.conn.Commit()
	return
}


func (This *clickhouseDB) GetTableFields(TableName string) (data []ckFieldStruct) {
	This.conn.Begin()
	stmt, _ := This.conn.Prepare("DESC TABLE "+TableName)
	rows, err := stmt.Query([]driver.Value{})
	if err != nil {
		This.err = err
		return
	}

	defer rows.Close()
	row := make([]driver.Value, 4)

	for rows.Next(row) == nil {
		var (
			Name            string
			Type           	string
			default_type	string
			default_expression string
		)
		Name = row[0].(string)
		Type = row[1].(string)
		default_type = row[2].(string)
		default_expression = row[3].(string)
		data = append(data,ckFieldStruct{Name:Name,Type:Type,DefaultType:default_type,DefaultExpression:default_expression})
	}
	This.err = This.conn.Commit()
	return
}
