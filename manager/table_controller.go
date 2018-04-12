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
	"net/http"
	"github.com/Bifrost/server"
	"github.com/Bifrost/toserver"
	"fmt"
	"strings"
	"encoding/json"
)

func init(){
	AddRoute("/table/del",table_del_controller)
	AddRoute("/table/add",table_add_controller)
	AddRoute("/table/toserverlist",table_toserverlist_controller)
	AddRoute("/table/deltoserver",table_delToServer_controller)
	AddRoute("/table/addtoserver",table_addToServer_controller)
}

func table_add_controller(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	tablename := req.Form.Get("table_name")
	schema := req.Form.Get("schema_name")
	channelId := GetFormInt(req,"channelid")
	err := server.AddTable(dbname,schema,tablename,channelId)
	if err != nil{
		w.Write(returnResult(false,err.Error()))
	}else{
		w.Write(returnResult(true,"success"))
	}
}

func table_del_controller(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	tablename := req.Form.Get("table_name")
	schema := req.Form.Get("schema_name")
	err := server.DelTable(dbname,schema,tablename)
	if err != nil{
		w.Write(returnResult(false,err.Error()))
	}else{
		w.Write(returnResult(true,"success"))
	}
}

func table_addToServer_controller(w http.ResponseWriter,req *http.Request){
	defer func() {
		if err := recover();err!=nil{
			w.Write([]byte(fmt.Sprint(err)))
		}
	}()
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	tablename := req.Form.Get("table_name")
	schema := req.Form.Get("schema_name")

	toServerKey := req.Form.Get("toserver_key")
	FieldListString := req.Form.Get("fieldlist")
	DataType := req.Form.Get("datatype")
	Type := req.Form.Get("type")
	MustBeSuccess := req.Form.Get("mustbe")
	AddEventType := req.Form.Get("add_eventtype")
	KeyConfig := req.Form.Get("key_config")
	ValConfig := req.Form.Get("val_config")
	AddSchemaName := req.Form.Get("add_schemaname")
	AddTableName := req.Form.Get("add_tablename")
	Expir := GetFormInt(req,"expir")

	var toServer server.ToServer

	if toserver.GetToServerInfo(toServerKey) == nil{
		w.Write(returnResult(false,toServerKey+"not exsit"))
		return
	}

	fileList := make([]string,0)

	if FieldListString != ""{
		for _,fieldName:= range strings.Split(FieldListString,","){
			fileList = append(fileList,fieldName)
		}
	}

	if KeyConfig == ""{
		w.Write(returnResult(false,"key_config must be"))
		return
	}

	if DataType == "string"{
		if ValConfig == ""{
			w.Write(returnResult(false,"DataType==stirng,"+ValConfig+" must be"))
			return
		}
	}else{
		if DataType != "json"{
			w.Write(returnResult(false,"DataType must be json or string"))
			return
		}
	}

	if Type != "set" && Type != "list"{
		w.Write(returnResult(false,"type must be set or list"))
		return
	}

	var MustBeSuccessBool , AddEventTypeBool,AddSchemaNameBool,AddTableNameBool bool = false,false,false,false
	if MustBeSuccess == "1"{
		MustBeSuccessBool = true
	}
	if AddEventType == "1"{
		AddEventTypeBool = true
	}
	if AddSchemaName == "1"{
		AddSchemaNameBool = true
	}
	if AddTableName == "1"{
		AddTableNameBool = true
	}
	toServer = server.ToServer{
		MustBeSuccess: MustBeSuccessBool,
		Type:          Type,
		DataType:	   DataType,
		KeyConfig:     KeyConfig,
		ValueConfig:   ValConfig,
		ToServerKey:   toServerKey,
		FieldList:     fileList,
		AddEventType:  AddEventTypeBool,
		AddSchemaName: AddSchemaNameBool,
		AddTableName:  AddTableNameBool,
		Expir:		   Expir,
	}
	dbObj := server.GetDBObj(dbname)
	r := dbObj.AddTableToServer(schema,tablename,toServer)
	if r == true{
		w.Write(returnResult(true,"success"))
	}else{
		w.Write(returnResult(false,"unkown error"))
	}
}

func table_delToServer_controller(w http.ResponseWriter,req *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			w.Write([]byte(fmt.Sprint(err)))
		}
	}()
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	tablename := req.Form.Get("table_name")
	schema := req.Form.Get("schema_name")
	index :=  GetFormInt(req,"index")
	server.GetDBObj(dbname).DelTableToServer(schema,tablename,index)
	w.Write(returnResult(true,"success"))
}

func table_toserverlist_controller(w http.ResponseWriter,req *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			w.Write([]byte(fmt.Sprint(err)))
		}
	}()
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	tablename := req.Form.Get("table_name")
	schema := req.Form.Get("schema_name")
	tableObj := server.GetDBObj(dbname).GetTable(schema,tablename)
	toserverList := tableObj.ToServerList
	b,_:=json.Marshal(toserverList)
	w.Write(b)
}

