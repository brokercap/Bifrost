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
	pluginStorage "github.com/brokercap/Bifrost/plugin/storage"
	"github.com/brokercap/Bifrost/server"
	"net/http"
	"strings"
)

func init(){
	addRoute("/table/del",table_del_controller)
	addRoute("/table/add",table_add_controller)
	addRoute("/table/update",table_update_controller)
	addRoute("/table/toserver/list",table_toserverlist_controller)
	addRoute("/table/toserver/del",table_delToServer_controller)
	addRoute("/table/toserver/add",table_addToServer_controller)
	addRoute("/table/toserver/deal",table_toserver_deal_controller)
	addRoute("/table/toserver/stop",table_toserver_stop_controller)
	addRoute("/table/toserver/start",table_toserver_start_controller)
}

func table_add_controller(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	tablename := req.Form.Get("table_name")
	schema := req.Form.Get("schema_name")
	IgnoreTable := req.Form.Get("ignore_table")
	channelId := GetFormInt(req,"channelid")
	schema = tansferSchemaName(schema)
	tablename = tansferTableName(tablename)
	err := server.AddTable(dbname,schema,tablename,IgnoreTable,channelId)
	if err != nil{
		w.Write(returnResult(false,err.Error()))
	}else{
		defer server.SaveDBConfigInfo()
		w.Write(returnResult(true,"success"))
	}
}

func table_update_controller(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	tablename := req.Form.Get("table_name")
	schema := req.Form.Get("schema_name")
	IgnoreTable := req.Form.Get("ignore_table")
	schema = tansferSchemaName(schema)
	tablename = tansferTableName(tablename)
	err := server.UpdateTable(dbname,schema,tablename,IgnoreTable)
	if err != nil{
		w.Write(returnResult(false,err.Error()))
	}else{
		defer server.SaveDBConfigInfo()
		w.Write(returnResult(true,"success"))
	}
}

func table_del_controller(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	tablename := req.Form.Get("table_name")
	schema := req.Form.Get("schema_name")
	schema = tansferSchemaName(schema)
	tablename = tansferTableName(tablename)
	err := server.DelTable(dbname,schema,tablename)
	if err != nil{
		w.Write(returnResult(false,err.Error()))
	}else{
		defer server.SaveDBConfigInfo()
		w.Write(returnResult(true,"success"))
	}
}

func table_addToServer_controller(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	tablename := req.Form.Get("table_name")
	schema := req.Form.Get("schema_name")

	schema = tansferSchemaName(schema)
	tablename = tansferTableName(tablename)

	toServerKey := req.Form.Get("toserver_key")
	PluginName := req.Form.Get("plugin_name")
	FieldListString := req.Form.Get("fieldlist")
	MustBeSuccess := req.Form.Get("mustbe")
	FilterQuery := req.Form.Get("FilterQuery")
	FilterUpdate := req.Form.Get("FilterUpdate")

	p  := req.Form.Get("param")
	var pluginParam map[string]interface{}
	err := json.Unmarshal([]byte(p),&pluginParam)
	if err != nil{
		w.Write(returnResult(false,err.Error()))
		return
	}

	if pluginStorage.GetToServerInfo(toServerKey) == nil{
		w.Write(returnResult(false,toServerKey+"not exsit"))
		return
	}

	fileList := make([]string,0)

	if FieldListString != ""{
		for _,fieldName:= range strings.Split(FieldListString,","){
			fileList = append(fileList,fieldName)
		}
	}

	var MustBeSuccessBool bool = false
	if MustBeSuccess == "1" || MustBeSuccess == "true"{
		MustBeSuccessBool = true
	}

	var FilterQueryBool bool = false
	if FilterQuery == "1" || FilterQuery == "true"{
		FilterQueryBool = true
	}

	var FilterUpdateBool bool = false
	if FilterUpdate == "1" || FilterUpdate == "true"{
		FilterUpdateBool = true
	}

	toServer := &server.ToServer{
		MustBeSuccess: 	MustBeSuccessBool,
		FilterQuery: 	FilterQueryBool,
		FilterUpdate: 	FilterUpdateBool,
		ToServerKey:   	toServerKey,
		PluginName:  	PluginName,
		FieldList:     	fileList,
		BinlogFileNum: 	0,
		BinlogPosition:	0,
		PluginParam:	pluginParam,
	}
	dbObj := server.GetDBObj(dbname)
	r,ToServerId := dbObj.AddTableToServer(schema,tablename,toServer)
	if r == true{
		defer server.SaveDBConfigInfo()
		w.Write(returnDataResult(true,"success",ToServerId))
	}else{
		w.Write(returnResult(false,"unkown error"))
	}
}

func table_delToServer_controller(w http.ResponseWriter,req *http.Request) {
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	tablename := req.Form.Get("table_name")
	schema := req.Form.Get("schema_name")

	schema = tansferSchemaName(schema)
	tablename = tansferTableName(tablename)

	ToServerID := GetFormInt(req,"to_server_id")
	server.GetDBObj(dbname).DelTableToServer(schema,tablename,ToServerID)
	defer server.SaveDBConfigInfo()
	w.Write(returnResult(true,"success"))
}

func table_toserverlist_controller(w http.ResponseWriter,req *http.Request) {
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	tablename := req.Form.Get("table_name")
	schema := req.Form.Get("schema_name")

	schema = tansferSchemaName(schema)
	tablename = tansferTableName(tablename)

	t1:=server.GetDBObj(dbname)
	t :=t1.GetTableSelf(schema,tablename)
	b,_:=json.Marshal(t.ToServerList)
	w.Write(b)
}

func table_toserver_deal_controller(w http.ResponseWriter,req *http.Request) {
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	tablename := req.Form.Get("table_name")
	schema := req.Form.Get("schema_name")

	schema = tansferSchemaName(schema)
	tablename = tansferTableName(tablename)

	ToServerID := GetFormInt(req,"to_server_id")
	index :=  GetFormInt(req,"index")
	t := server.GetDBObj(dbname).GetTableSelf(schema,tablename)
	ToServerInfo := t.ToServerList[index]
	if ToServerInfo != nil && ToServerInfo.ToServerID == ToServerID{
		ToServerInfo.DealWaitError()
	}
	w.Write(returnResult(true,"success"))
}

func table_toserver_stop_controller(w http.ResponseWriter,req *http.Request) {
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	tablename := req.Form.Get("table_name")
	schema := req.Form.Get("schema_name")

	schema = tansferSchemaName(schema)
	tablename = tansferTableName(tablename)

	ToServerID := GetFormInt(req,"to_server_id")
	index :=  GetFormInt(req,"index")
	t := server.GetDBObj(dbname).GetTable(schema,tablename)
	ToServerInfo := t.ToServerList[index]
	if ToServerInfo != nil && ToServerInfo.ToServerID == ToServerID{
		ToServerInfo.Stop()
	}
	w.Write(returnResult(true,"success"))
}

func table_toserver_start_controller(w http.ResponseWriter,req *http.Request) {
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	tablename := req.Form.Get("table_name")
	schema := req.Form.Get("schema_name")

	schema = tansferSchemaName(schema)
	tablename = tansferTableName(tablename)

	ToServerID := GetFormInt(req,"to_server_id")
	index :=  GetFormInt(req,"index")
	t := server.GetDBObj(dbname).GetTable(schema,tablename)
	ToServerInfo := t.ToServerList[index]
	if ToServerInfo != nil && ToServerInfo.ToServerID == ToServerID{
		ToServerInfo.Start()
	}
	w.Write(returnResult(true,"success"))
}

