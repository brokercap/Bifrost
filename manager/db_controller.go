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
	"github.com/brokercap/Bifrost/server"
	"strconv"
	"encoding/json"
	"html/template"
	"fmt"
	"log"
	"runtime/debug"
	"strings"
	"time"
)

func init(){
	addRoute("/db/add",addDB_Action)
	addRoute("/db/update",updateDB_Action)
	addRoute("/db/stop",stopDB_Action)
	addRoute("/db/start",startDB_Action)
	addRoute("/db/close",closeDB_Action)
	addRoute("/db/del",delDB_Action)
	addRoute("/db/list",listDB_Action)
	addRoute("/db/check_uri",check_db_connect_Action)
}

type dbListStruct struct{
	TemplateHeader
	DBList map[string]server.DbListStruct
}

func listDB_Action(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	if req.Form.Get("format") == "json"{
		data,_:=json.Marshal(server.GetListDb())
		w.Write(data)
		return
	}
	DbList := dbListStruct{DBList:server.GetListDb()}
	DbList.Title = "Bifrost"
	t, _ := template.ParseFiles(TemplatePath("manager/template/db.list.html"),TemplatePath("manager/template/header.html"),TemplatePath("manager/template/footer.html"))
	t.Execute(w, DbList)
}


func addDB_Action(w http.ResponseWriter,req *http.Request){
	var result resultStruct
	result.Status = false
	req.ParseForm()
	dbname := strings.Trim(req.Form.Get("dbname"),"")
	connuri := strings.Trim(req.Form.Get("uri"),"")
	filename := strings.Trim(req.Form.Get("filename"),"")
	positionString := strings.Trim(req.Form.Get("position"),"")
	serverIdString := strings.Trim(req.Form.Get("serverid"),"")
	max_filename := strings.Trim(req.Form.Get("max_filename"),"")
	max_position := uint32(GetFormInt(req,"max_position"))

	position,err:=strconv.Atoi(positionString)
	if err != nil {
		result.Msg = "position is err"
	}
	serverId,err:=strconv.Atoi(serverIdString)
	if err != nil {
		result.Msg += "serverid is err"
	}
	if result.Msg != ""{
		data,_:=json.Marshal(result)
		w.Write(data)
	}else{
		defer server.SaveDBConfigInfo()
		server.AddNewDB(dbname,connuri,filename,uint32(position),uint32(serverId),max_filename,max_position,time.Now().Unix())
		server.GetDBObj(dbname).AddChannel("default",1)
		data,_:=json.Marshal(resultStruct{Status:true,Msg:"success"})
		w.Write(data)
	}
}

func updateDB_Action(w http.ResponseWriter,req *http.Request){
	var result resultStruct
	result.Status = false
	req.ParseForm()
	dbname := strings.Trim(req.Form.Get("dbname"),"")
	connuri := strings.Trim(req.Form.Get("uri"),"")
	filename := strings.Trim(req.Form.Get("filename"),"")
	positionString := strings.Trim(req.Form.Get("position"),"")
	serverIdString := strings.Trim(req.Form.Get("serverid"),"")
	max_filename := strings.Trim(req.Form.Get("max_filename"),"")
	max_position := uint32(GetFormInt(req,"max_position"))

	position,err:=strconv.Atoi(positionString)
	if err != nil {
		result.Msg = "position is err"
	}
	serverId,err:=strconv.Atoi(serverIdString)
	if err != nil {
		result.Msg += "serverid is err"
	}
	if result.Msg != ""{
		data,_:=json.Marshal(result)
		w.Write(data)
	}else{
		defer server.SaveDBConfigInfo()
		err := server.UpdateDB(dbname,connuri,filename,uint32(position),uint32(serverId),max_filename,max_position,time.Now().Unix())
		var data []byte
		if err == nil{
			data,_ =json.Marshal(resultStruct{Status:true,Msg:"success"})
		}else{
			data,_ =json.Marshal(resultStruct{Status:false,Msg:err.Error()})
		}
		w.Write(data)
	}
}


func delDB_Action(w http.ResponseWriter,req *http.Request){
	var result resultStruct
	result.Status = false
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	r := server.DelDB(dbname)
	if r == true{
		result.Status = true
		result.Msg = "success"
	}else{
		result.Msg = "error"
	}
	defer server.SaveDBConfigInfo()
	data,_:=json.Marshal(result)
	w.Write(data)
}

func stopDB_Action(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	server.DbList[dbname].Stop()
	defer server.SaveDBConfigInfo()
	data,_:=json.Marshal(resultStruct{Status:true,Msg:"success"})
	w.Write(data)
}

func startDB_Action(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	r := server.DbList[dbname].Start()
	if r != true {
		data,_:=json.Marshal(resultStruct{Status:false,Msg:"failed"})
		w.Write(data)
	}else{
		defer server.SaveDBConfigInfo()
		data,_:=json.Marshal(resultStruct{Status:true,Msg:"success"})
		w.Write(data)
	}
}

func closeDB_Action(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	r := server.DbList[dbname].Close()
	defer server.SaveDBConfigInfo()
	data,_:=json.Marshal(resultStruct{Status:r,Msg:""})
	w.Write(data)
}

func check_db_connect_Action(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	dbUri := req.Form.Get("uri")
	type dbInfoStruct struct{
		BinlogFile string
		BinlogPosition int
		ServerId int
		BinlogFormat string
	}
	dbInfo := &dbInfoStruct{}
	err := func(dbUri string) (e error){
		e = nil
		defer func() {
			if err := recover();err != nil{
				log.Println(string(debug.Stack()))
				e = fmt.Errorf(fmt.Sprint(err))
				return
			}
		}()
		dbconn := DBConnect(dbUri)
		if dbconn != nil{
			e = nil
		}else{
			e = fmt.Errorf("db conn ,uknow error")
		}
		defer dbconn.Close()
		MasterBinlogInfo := GetBinLogInfo(dbconn)
		if MasterBinlogInfo.File != ""{
			dbInfo.BinlogFile = MasterBinlogInfo.File
			dbInfo.BinlogPosition = MasterBinlogInfo.Position
			dbInfo.ServerId = GetServerId(dbconn)
			variablesMap := GetVariables(dbconn,"binlog_format")
			if _,ok := variablesMap["binlog_format"];!ok{
				dbInfo.BinlogFormat = variablesMap["binlog_format"]
			}
		}else{
			e = fmt.Errorf("The binlog maybe not open,or no replication client privilege(s).you can show log more.")
		}
		return
	}(dbUri)
	if err != nil{
		w.Write(returnDataResult(false,err.Error(),*dbInfo))
	}else{
		w.Write(returnDataResult(true,"success",*dbInfo))
	}
}
