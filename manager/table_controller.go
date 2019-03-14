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
	"github.com/jc3wish/Bifrost/server"
	"github.com/jc3wish/Bifrost/plugin"
	"strings"
	"encoding/json"
	"log"
)

func init(){
	addRoute("/table/del",table_del_controller)
	addRoute("/table/add",table_add_controller)
	addRoute("/table/toserverlist",table_toserverlist_controller)
	addRoute("/table/deltoserver",table_delToServer_controller)
	addRoute("/table/addtoserver",table_addToServer_controller)
	addRoute("/table/toserver/deal",table_toserver_deal_controller)
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
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	tablename := req.Form.Get("table_name")
	schema := req.Form.Get("schema_name")

	toServerKey := req.Form.Get("toserver_key")
	toserver_type := req.Form.Get("toserver_type")
	FieldListString := req.Form.Get("fieldlist")
	MustBeSuccess := req.Form.Get("mustbe")

	p  := req.Form.Get("param")
	var pluginParam map[string]interface{}
	err := json.Unmarshal([]byte(p),&pluginParam)
	log.Println("param",p)
	if err != nil{
		w.Write(returnResult(false,err.Error()))
		return
	}

	if plugin.GetToServerInfo(toServerKey) == nil{
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
	if MustBeSuccess == "1"{
		MustBeSuccessBool = true
	}

	toServer := &server.ToServer{
		MustBeSuccess: MustBeSuccessBool,
		ToServerKey:   toServerKey,
		ToServerType:  toserver_type,
		FieldList:     fileList,
		BinlogFileNum: 0,
		BinlogPosition:0,
		PluginParam:	pluginParam,
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
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	tablename := req.Form.Get("table_name")
	schema := req.Form.Get("schema_name")
	index :=  GetFormInt(req,"index")
	ToServerID := GetFormInt(req,"to_server_id")
	server.GetDBObj(dbname).DelTableToServer(schema,tablename,index,ToServerID)
	w.Write(returnResult(true,"success"))
}

func table_toserverlist_controller(w http.ResponseWriter,req *http.Request) {
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	tablename := req.Form.Get("table_name")
	schema := req.Form.Get("schema_name")
	t1:=server.GetDBObj(dbname)
	tableObj:=t1.GetTable(schema,tablename)
	//tableObj := server.GetDBObj(dbname).GetTable(schema,tablename)
	b,_:=json.Marshal(tableObj.ToServerList)
	w.Write(b)
}

func table_toserver_deal_controller(w http.ResponseWriter,req *http.Request) {
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	tablename := req.Form.Get("table_name")
	schema := req.Form.Get("schema_name")
	ToServerID := GetFormInt(req,"to_server_id")
	index :=  GetFormInt(req,"index")
	ToServerInfo := server.GetDBObj(dbname).GetTable(schema,tablename).ToServerList[index]
	if ToServerInfo.ToServerID == ToServerID{
		ToServerInfo.DealWaitError()
	}
	w.Write(returnResult(true,"success"))
}

