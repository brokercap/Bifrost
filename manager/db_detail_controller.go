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
package manager

import (
	"encoding/json"
	toserver "github.com/brokercap/Bifrost/plugin/storage"
	"github.com/brokercap/Bifrost/server"
	"html/template"
	"net/http"
	"strings"
)

func init() {
	addRoute("/db/detail", db_detail_controller)
	addRoute("/db/tablelist", get_table_List_controller)
	addRoute("/db/tablefields", get_table_fields_controller)
	addRoute("/db/table/createsql", get_table_create_sql_controller)
}

func db_detail_controller(w http.ResponseWriter, req *http.Request) {
	type dbDetail struct {
		TemplateHeader
		DbName       string
		DataBaseList []string
		ToServerList map[string]*toserver.ToServer
		ChannelList  map[int]*server.Channel
	}
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	dbUri := server.GetDBObj(dbname).ConnectUri
	dbConn := DBConnect(dbUri)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()
	DataBaseList := GetSchemaList(dbConn)
	DataBaseList = append(DataBaseList, "AllDataBases")
	var Result dbDetail
	Result = dbDetail{DataBaseList: DataBaseList, DbName: dbname, ToServerList: toserver.GetToServerMap(), ChannelList: server.GetDBObj(dbname).ListChannel()}
	Result.Title = dbname + " - Detail - Bifrost"
	t, _ := template.ParseFiles(TemplatePath("/manager/template/db.detail.html"), TemplatePath("/manager/template/header.html"), TemplatePath("/manager/template/db.detail.history.add.html"), TemplatePath("/manager/template/footer.html"))
	t.Execute(w, Result)
}

func get_table_List_controller(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	schema_name := req.Form.Get("schema_name")
	DBObj := server.GetDBObj(dbname)
	dbUri := DBObj.ConnectUri
	dbConn := DBConnect(dbUri)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()
	type ResultType struct {
		TableName   string
		ChannelName string
		AddStatus   bool
		TableType   string
	}
	var data []ResultType
	data = make([]ResultType, 0)
	TableList := GetSchemaTableList(dbConn, schema_name)
	TableList = append(TableList, TableListStruct{TableName: "AllTables", TableType: "LIKE"})
	var schema_name0, tableName0 string
	schema_name0 = tansferSchemaName(schema_name)

	for _, tableInfo := range TableList {
		tableName := tableInfo.TableName
		tableType := tableInfo.TableType
		tableName0 = tansferTableName(tableName)
		t := DBObj.GetTable(schema_name0, tableName0)
		if t == nil {
			data = append(data, ResultType{TableName: tableName, ChannelName: "", AddStatus: false, TableType: tableType})
		} else {
			t2 := DBObj.GetChannel(t.ChannelKey)
			if t2 == nil {
				data = append(data, ResultType{TableName: tableName, ChannelName: "", AddStatus: false, TableType: tableType})
			} else {
				data = append(data, ResultType{TableName: tableName, ChannelName: t2.Name, AddStatus: true, TableType: tableType})
			}
		}
	}
	if schema_name0 != "*" {
		for _, v := range DBObj.GetTables() {
			if strings.Index(v.Name, "*") <= 0 {
				continue
			}
			t2 := DBObj.GetChannel(v.ChannelKey)
			data = append(data, ResultType{TableName: v.Name, ChannelName: t2.Name, AddStatus: true, TableType: "LIKE"})
		}
	}
	b, _ := json.Marshal(data)
	w.Write(b)
}

func get_table_fields_controller(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	schema_name := req.Form.Get("schema_name")
	table_name := req.Form.Get("table_name")

	schema_name = tansferSchemaName(schema_name)
	table_name = tansferTableName(table_name)

	DBObj := server.GetDBObj(dbname)
	dbUri := DBObj.ConnectUri
	dbConn := DBConnect(dbUri)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()
	TableFieldsList := GetSchemaTableFieldList(dbConn, schema_name, table_name)
	b, _ := json.Marshal(TableFieldsList)
	w.Write(b)
}

func get_table_create_sql_controller(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	schema_name := req.Form.Get("schema_name")
	table_name := req.Form.Get("table_name")

	schema_name = tansferSchemaName(schema_name)
	table_name = tansferTableName(table_name)

	DBObj := server.GetDBObj(dbname)
	dbUri := DBObj.ConnectUri
	dbConn := DBConnect(dbUri)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()
	sql := ShowTableCreate(dbConn, schema_name, table_name)
	w.Write([]byte(sql))
}
