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
	"strconv"
	"html/template"
	"encoding/json"
	"github.com/brokercap/Bifrost/server/warning"
	"strings"
)

func init(){
	addRoute("/warning/config/list",warning_config_list_controller)
	addRoute("/warning/config/add",warning_config_add_controller)
	addRoute("/warning/config/del",warning_config_del_controller)
	addRoute("/warning/config/check",warning_config_check_controller)
}

func warning_config_add_controller(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	var result resultStruct
	result.Status = false
	Type := req.Form.Get("type")
	p  := req.Form.Get("param")
	var WarningParam map[string]interface{}
	err := json.Unmarshal([]byte(p),&WarningParam)
	if err != nil{
		w.Write(returnResult(false,err.Error()))
		return
	}
	id,err := warning.AddNewWarningConfig(warning.WaringConfig{Type:Type,Param:WarningParam})
	if err != nil{
		w.Write(returnDataResult(false,err.Error(),""))
	}else{
		w.Write(returnDataResult(true,"success",id))
	}
	return
}

func warning_config_check_controller(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	var result resultStruct
	result.Status = false
	Type := req.Form.Get("type")
	p  := req.Form.Get("param")
	var WarningParam map[string]interface{}
	err := json.Unmarshal([]byte(p),&WarningParam)
	if err != nil{
		w.Write(returnResult(false,err.Error()))
		return
	}
	err = warning.CheckWarngConfigBySendTest(warning.WaringConfig{Type:Type,Param:WarningParam},"it is test")
	if err != nil{
		w.Write(returnResult(false,err.Error()))
		return
	}
	w.Write(returnResult(true,"success"))
	return
}

func warning_config_list_controller(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	warningConfigMap := warning.GetWarningConfigList()
	if req.Form.Get("format") == "json"{
		data,_:=json.Marshal(warningConfigMap)
		w.Write(data)
		return
	}
	type warningResult struct {
		TemplateHeader
		DbName string
		WaringConfigList map[string]warning.WaringConfig
	}
	var data warningResult
	data = warningResult{WaringConfigList:warningConfigMap}
	data.Title = " - Warning Config List - Bifrost"
	t, _ := template.ParseFiles(TemplatePath("manager/template/warning.config.list.html"),TemplatePath("manager/template/header.html"),TemplatePath("manager/template/footer.html"))
	t.Execute(w, data)
}


func warning_config_del_controller(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	idkey := req.Form.Get("id")
	tmp := strings.Split(idkey,"_")
	idString := tmp[len(tmp)-1]
	id,err := strconv.Atoi(idString)
	if err != nil{
		w.Write(returnResult(false,"id error:"+idkey))
		return
	}
	warning.DelWarningConfig(id)
	w.Write(returnResult(true,"success"))
	return
}