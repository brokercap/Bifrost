package src

import (
	"database/sql"
	_ "github.com/kshvakov/clickhouse"
	"github.com/jc3wish/Bifrost/manager/xgo"
	"net/http"
	"encoding/json"
	pluginStorage "github.com/jc3wish/Bifrost/plugin/storage"
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
	c := newClickHouseDBConn(toServerInfo.ConnUri)
	defer c.Close()
	m := c.getSchemaList()
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
	c := newClickHouseDBConn(toServerInfo.ConnUri)
	defer c.Close()
	m := c.getSchemaTableList(schema)
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
	c := newClickHouseDBConn(toServerInfo.ConnUri)
	defer c.Close()
	m := c.getTableFields(schema+"."+TableName)
	b,_:=json.Marshal(m)
	w.Write(b)
	return
}

func newClickHouseDBConn(uri string) *clickhouseDB {
	c := &clickhouseDB{
		uri:uri,
	}
	c.Open()
	return c
}

type clickhouseDB struct {
	uri 	string
	conn 	*sql.DB
	err 	error
}

func(This *clickhouseDB) Open() bool{
	This.conn, This.err = sql.Open("clickhouse", This.uri)
	return true
}

func(This *clickhouseDB) Close() bool{
	if This.conn != nil{
		This.conn.Close()
	}
	return true
}

func (This *clickhouseDB) getSchemaList() (data []string) {
	rows,err := This.conn.Query("SHOW DATABASES")
	if err != nil {
		This.err = err
		return
	}
	defer rows.Close()
	for rows.Next() {
		var (
			Name            string
		)
		if err := rows.Scan(&Name); err != nil {
			This.err = err
			return
		}
		data = append(data,Name)
	}
	return
}


func (This *clickhouseDB) getSchemaTableList(schema string) (data []string) {
	if schema == ""{
		return
	}
	rows,err := This.conn.Query("select name from system.tables where database = '"+schema+"'")
	if err != nil {
		This.err = err
		return
	}
	defer rows.Close()
	for rows.Next() {
		var (
			Name            string

		)
		if err := rows.Scan(&Name); err != nil {
			This.err = err
			return
		}
		data = append(data,Name)
	}
	return
}


func (This *clickhouseDB) getTableFields(TableName string) (data []ckFieldStruct) {
	rows,err := This.conn.Query("DESC TABLE "+TableName)
	if err != nil {
		This.err = err
		return
	}
	defer rows.Close()
	for rows.Next() {
		var (
			Name            string
			Type           	string
			default_type	string
			default_expression string
		)
		if err := rows.Scan(&Name, &Type,&default_type,&default_expression); err != nil {
			This.err = err
			return
		}
		data = append(data,ckFieldStruct{Name:Name,Type:Type,DefaultType:default_type,DefaultExpression:default_expression})
	}
	return
}
