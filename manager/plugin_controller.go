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
	"github.com/jc3wish/Bifrost/plugin/driver"
	"github.com/jc3wish/Bifrost/plugin"
	"html/template"
)

func init()  {
	addRoute("/plugin/list",plugin_list_controller)
	addRoute("/plugin/reload",plugin_reload_controller)
}

func plugin_list_controller(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	type PluginListInfo struct {
		TemplateHeader
		Drivers []map[string]string
	}
	var data PluginListInfo
	data = PluginListInfo{Drivers:driver.Drivers()}
	data.Title = "Plugin List - Bifrost"
	t, _ := template.ParseFiles(TemplatePath("manager/template/plugin.list.html"),TemplatePath("manager/template/header.html"),TemplatePath("manager/template/footer.html"))
	t.Execute(w, data)

}

func plugin_reload_controller(w http.ResponseWriter,req *http.Request){
	err := plugin.LoadPlugin()
	if err != nil{
		w.Write(returnResult(false,err.Error()))
	}else{
		w.Write(returnResult(true,"success"))
	}
}
