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
	toserver "github.com/brokercap/Bifrost/plugin/storage"
	"github.com/brokercap/Bifrost/plugin/driver"
	"github.com/brokercap/Bifrost/server"
	"html/template"
	"encoding/json"
	"strings"
)

func init()  {
	addRoute("/toserver/add",toserver_add_controller)
	addRoute("/toserver/update",toserver_update_controller)
	addRoute("/toserver/del",toserver_del_controller)
	addRoute("/toserver/list",toserver_list_controller)
	addRoute("/toserver/check_uri",toserver_checkuri_controller)
}

func toserver_add_controller(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	toServerName := req.Form.Get("toserverkey")
	Type := req.Form.Get("type")
	Notes := req.Form.Get("notes")
	ConnUri := req.Form.Get("connuri")
	MaxConn := GetFormInt(req,"maxconn")
	if toServerName == "" || Type == "" || ConnUri==""{
		w.Write(returnResult(false,"toserverkey,type,connuri muest be not empty"))
		return
	}
	toserver.SetToServerInfo(
		toServerName,
		toserver.ToServer{
			PluginName:Type,
			ConnUri:ConnUri,
			Notes:Notes,
			MaxConn:MaxConn,
			})
	defer server.SaveDBConfigInfo()
	w.Write(returnResult(true,"success"))
}

func toserver_list_controller(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	if req.Form.Get("format") == "json"{
		data,_:=json.Marshal(toserver.ToServerMap)
		w.Write(data)
		return
	}
	type toServerInfo struct {
		TemplateHeader
		ToServerList map[string]*toserver.ToServer
		Drivers map[string]driver.DriverStructure
	}
	var data toServerInfo
	data = toServerInfo{ToServerList: toserver.ToServerMap,Drivers:driver.Drivers()}
	data.Title = "ToServer List - Bifrost"
	t, _ := template.ParseFiles(TemplatePath("manager/template/toserver.list.html"),TemplatePath("manager/template/header.html"),TemplatePath("manager/template/footer.html"))
	t.Execute(w, data)

}

func toserver_del_controller(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	toServerName := req.Form.Get("toserverkey")
	toserver.DelToServerInfo(toServerName)
	defer server.SaveDBConfigInfo()
	w.Write(returnResult(true,"success"))
}

func toserver_checkuri_controller(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	Type := req.Form.Get("type")
	ConnUri := req.Form.Get("connuri")
	if Type == "" || ConnUri==""{
		w.Write(returnResult(false,"type,connuri must be not empty"))
		return
	}
	err := driver.CheckUri(Type,ConnUri)
	if err !=nil{
		w.Write(returnResult(false,err.Error()))
		return
	}
	w.Write(returnResult(true,"success"))
}

func toserver_update_controller(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	toServerName := strings.Trim(req.Form.Get("toserverkey"),"")
	plugin := req.Form.Get("plugin")
	Notes := req.Form.Get("notes")
	ConnUri := req.Form.Get("connuri")
	MaxConn := GetFormInt(req,"maxconn")
	if toServerName == "" || plugin == "" || ConnUri==""{
		w.Write(returnResult(false,"toserverkey,plugin,connuri muest be not empty"))
		return
	}
	toserver.UpdateToServerInfo(
		toServerName,
		toserver.ToServer{
			PluginName:plugin,
			ConnUri:ConnUri,
			Notes:Notes,
			MaxConn:MaxConn,
		})
	defer server.SaveDBConfigInfo()
	w.Write(returnResult(true,"success"))
}