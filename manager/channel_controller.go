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
	"fmt"
	"github.com/brokercap/Bifrost/server"
	"html/template"
	"net/http"
	"strconv"
)

func init() {
	addRoute("/channel/list", channle_list_controller)
	addRoute("/channel/add", channle_add_controller)
	addRoute("/channel/stop", channle_stop_controller)
	addRoute("/channel/start", channle_start_controller)
	addRoute("/channel/del", channle_del_controller)
	addRoute("/channel/close", channle_close_controller)
	addRoute("/channel/tablelist", channle_tablelist_controller)
}

func channle_add_controller(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	var result resultStruct
	result.Status = false
	dbname := req.Form.Get("dbname")
	chname := req.Form.Get("channel_name")
	cosumercountString := req.Form.Get("cosumercount")
	cosumercount, err := strconv.Atoi(cosumercountString)
	if err != nil {
		w.Write(returnResult(false, err.Error()))
		return
	}
	db := server.GetDBObj(dbname)
	if db == nil {
		w.Write(returnResult(false, dbname+" not exsit"))
		return
	}
	_, ChannelID := db.AddChannel(chname, cosumercount)
	defer server.SaveDBConfigInfo()
	w.Write(returnDataResult(true, "success", ChannelID))
}

func channle_list_controller(w http.ResponseWriter, req *http.Request) {
	type channelResult struct {
		TemplateHeader
		DbName      string
		ChannelList map[int]*server.Channel
	}
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	if req.Form.Get("format") == "json" {
		data, _ := json.Marshal(server.GetDBObj(dbname).ListChannel())
		w.Write(data)
		return
	}
	var data channelResult
	data = channelResult{ChannelList: server.GetDBObj(dbname).ListChannel(), DbName: dbname}
	data.Title = dbname + " - Channel List - Bifrost"
	t, _ := template.ParseFiles(TemplatePath("/manager/template/channel.list.html"), TemplatePath("/manager/template/header.html"), TemplatePath("/manager/template/footer.html"))
	t.Execute(w, data)
}

func channle_stop_controller(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	channelIDString := req.Form.Get("channelid")
	channelID, _ := strconv.Atoi(channelIDString)
	ch := server.GetChannel(dbname, channelID)
	if ch == nil {
		w.Write(returnResult(false, "channel not exsit"))
		return
	}
	ch.Stop()
	defer server.SaveDBConfigInfo()
	w.Write(returnResult(true, "success"))
	return
}

func channle_close_controller(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	channelIDString := req.Form.Get("channelid")
	channelID, _ := strconv.Atoi(channelIDString)
	ch := server.GetChannel(dbname, channelID)
	if ch == nil {
		w.Write(returnResult(false, "channel not exsit"))
		return
	}
	ch.Close()
	defer server.SaveDBConfigInfo()
	w.Write(returnResult(true, "success"))
	return
}

func channle_start_controller(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	channelIDString := req.Form.Get("channelid")
	channelID, _ := strconv.Atoi(channelIDString)
	ch := server.GetChannel(dbname, channelID)
	if ch == nil {
		w.Write(returnResult(false, "channel not exsit"))
		return
	}
	ch.Start()
	defer server.SaveDBConfigInfo()
	w.Write(returnResult(true, "success"))
	return
}

func channle_del_controller(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	channelID := GetFormInt(req, "channelid")
	db := server.GetDBObj(dbname)
	TableMap := db.GetTableByChannelKey(dbname, channelID)
	n := len(TableMap)
	if len(TableMap) > 0 {
		w.Write(returnResult(false, "The channel bind table count:"+fmt.Sprint(n)))
		return
	}
	r := server.DelChannel(dbname, channelID)
	if r == true {
		defer server.SaveDBConfigInfo()
		w.Write(returnResult(true, "success"))
	} else {
		w.Write(returnResult(false, "channel or db not exsit"))
	}
	return
}

func channle_tablelist_controller(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	channelID := GetFormInt(req, "channelid")
	channelInfo := server.GetChannel(dbname, channelID)
	if channelInfo == nil {
		w.Write(returnResult(false, "channel not exsit"))
		return
	}
	db := server.GetDBObj(dbname)
	type channelTableResult struct {
		TemplateHeader
		ChannelID   int
		DbName      string
		ChannelName string
		TableList   map[string]*server.Table
	}
	TableMap := db.GetTableByChannelKey(dbname, channelID)
	var data channelTableResult
	data = channelTableResult{
		TableList:   TableMap,
		DbName:      dbname,
		ChannelName: channelInfo.Name,
		ChannelID:   channelID,
	}
	data.Title = dbname + " - Table List - Channel - Bifrost"
	t, _ := template.ParseFiles(TemplatePath("/manager/template/channel.table.list.html"), TemplatePath("/manager/template/header.html"), TemplatePath("/manager/template/footer.html"))
	t.Execute(w, data)
}
