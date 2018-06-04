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
	"strconv"
	"log"
	"html/template"
)

func init(){
	AddRoute("/channel/list",channle_list_controller)
	AddRoute("/channel/add",channle_add_controller)
	AddRoute("/channel/stop",channle_stop_controller)
	AddRoute("/channel/start",channle_start_controller)
	AddRoute("/channel/del",channle_del_controller)
	AddRoute("/channel/close",channle_close_controller)
	AddRoute("/channel/deal",channle_deal_controller)
}

func channle_add_controller(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	var result resultStruct
	result.Status = false
	dbname := req.Form.Get("dbname")
	chname := req.Form.Get("channel_name")
	cosumercountString := req.Form.Get("cosumercount")
	cosumercount,err:=strconv.Atoi(cosumercountString)
	if err != nil{
		w.Write(returnResult(false, err.Error()))
		return
	}
	db := server.GetDBObj(dbname)
	if db == nil {
		w.Write(returnResult(false,dbname+" not exsit"))
		return
	}
	db.AddChannel(chname,cosumercount)
	w.Write(returnResult(true,"success"))
}

func channle_list_controller(w http.ResponseWriter,req *http.Request){
	defer func() {
		if err := recover();err!=nil{
			log.Println(err)
		}
	}()
	type channelResult struct {
		TemplateHeader
		DbName string
		ChannelList map[int]*server.Channel
	}
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	var data channelResult
	data = channelResult{ChannelList:server.GetDBObj(dbname).ListChannel(),DbName:dbname}
	data.Title = dbname +" - Channel List - Bifrost"
	t, _ := template.ParseFiles(TemplatePath("manager/template/channel.list.html"),TemplatePath("manager/template/header.html"),TemplatePath("manager/template/footer.html"))
	t.Execute(w, data)
}

func channle_stop_controller(w http.ResponseWriter,req *http.Request){
	defer func() {
		if err := recover();err!=nil{
			log.Println(err)
			return
		}
	}()
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	channelIDString := req.Form.Get("channelid")
	channelID,_:=strconv.Atoi(channelIDString)
	ch := server.GetChannel(dbname,channelID)
	if ch == nil{
		w.Write(returnResult(false,"channel not exsit"))
		return
	}
	ch.Stop()
	w.Write(returnResult(true,"success"))
	return
}

func channle_close_controller(w http.ResponseWriter,req *http.Request){
	defer func() {
		if err := recover();err!=nil{
			log.Println(err)
			return
		}
	}()
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	channelIDString := req.Form.Get("channelid")
	channelID,_:=strconv.Atoi(channelIDString)
	ch := server.GetChannel(dbname,channelID)
	if ch == nil{
		w.Write(returnResult(false,"channel not exsit"))
		return
	}
	ch.Close()
	w.Write(returnResult(true,"success"))
	return
}

func channle_start_controller(w http.ResponseWriter,req *http.Request){
	defer func() {
		if err := recover();err!=nil{
			return
		}
	}()
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	channelIDString := req.Form.Get("channelid")
	channelID,_:=strconv.Atoi(channelIDString)
	ch := server.GetChannel(dbname,channelID)
	if ch == nil{
		w.Write(returnResult(false,"channel not exsit"))
		return
	}
	ch.Start()
	w.Write(returnResult(true,"success"))
	return
}

func channle_deal_controller(w http.ResponseWriter,req *http.Request){
	defer func() {
		if err := recover();err!=nil{
			return
		}
	}()
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	channelIDString := req.Form.Get("channelid")
	channelID,_:=strconv.Atoi(channelIDString)
	ch := server.GetChannel(dbname,channelID)
	if ch == nil{
		w.Write(returnResult(false,"channel not exsit"))
		return
	}
	errorid:= GetFormInt(req,"error_id")
	ch.DealWaitError(errorid)
	w.Write(returnResult(true,"success"))
	return
}

func channle_del_controller(w http.ResponseWriter,req *http.Request){
	defer func() {
		if err := recover();err!=nil{
			return
		}
	}()
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	channelID:= GetFormInt(req,"channelid")
	r := server.DelChannel(dbname,channelID)
	if r == true{
		w.Write(returnResult(true,"success"))
	}else{
		w.Write(returnResult(false,"channel or db not exsit"))
	}
	return
}