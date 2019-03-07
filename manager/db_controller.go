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
	"github.com/jc3wish/Bifrost/toserver"
	"strconv"
	"encoding/json"
	"html/template"
	"fmt"
	"log"
	"runtime/debug"
	"strings"
)

func init(){
	AddRoute("/db/add",addDB_Action)
	AddRoute("/db/stop",stopDB_Action)
	AddRoute("/db/start",startDB_Action)
	AddRoute("/db/close",closeDB_Action)
	AddRoute("/db/del",delDB_Action)
	AddRoute("/db/list",listDB_Action)
	AddRoute("/db/check_uri",check_db_connect_Action)
}

type dbListStruct struct{
	TemplateHeader
	DBList map[string]server.DbListStruct
}

func listDB_Action(w http.ResponseWriter,req *http.Request){
	if len(toserver.ToServerMap) == 0{
		http.Redirect(w, req, "/toserver/list", http.StatusFound)
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
		server.AddNewDB(dbname,connuri,filename,uint32(position),uint32(serverId),max_filename,max_position)
		server.GetDBObj(dbname).AddChannel("default",1)
		data,_:=json.Marshal(resultStruct{Status:true,Msg:"success"})
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
	data,_:=json.Marshal(result)
	w.Write(data)
}

func stopDB_Action(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	server.DbList[dbname].Stop()
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
		data,_:=json.Marshal(resultStruct{Status:true,Msg:"success"})
		w.Write(data)
	}
}

func closeDB_Action(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	r := server.DbList[dbname].Close()
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
		}else{
			e = fmt.Errorf("The binlog maybe not open")
		}
		return
	}(dbUri)
	if err != nil{
		w.Write(returnDataResult(false,err.Error(),*dbInfo))
	}else{
		w.Write(returnDataResult(true,"success",*dbInfo))
	}
}
