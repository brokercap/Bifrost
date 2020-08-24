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
	"github.com/brokercap/Bifrost/plugin"
	"github.com/brokercap/Bifrost/plugin/driver"
	"html/template"
	"net/http"
)

func init() {
	addRoute("/plugin/list", plugin_list_controller)
	addRoute("/plugin/reload", plugin_reload_controller)
}

func plugin_list_controller(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	type PluginListInfo struct {
		TemplateHeader
		PluginAPIVersion string
		Drivers          map[string]driver.DriverStructure
	}
	var data PluginListInfo
	data = PluginListInfo{
		PluginAPIVersion: driver.GetApiVersion(),
		Drivers:          driver.Drivers(),
	}

	//因为plugin 加载so 插件，有可能会异常，所以这里需要你把异常的插件列表也加载进来并进行显示出来
	errorPluginMap := plugin.GetErrorPluginList()
	for name, v := range errorPluginMap {
		data.Drivers[name] = v
	}

	data.Title = "Plugin List - Bifrost"
	t, _ := template.ParseFiles(TemplatePath("/manager/template/plugin.list.html"), TemplatePath("/manager/template/header.html"), TemplatePath("/manager/template/footer.html"))
	t.Execute(w, data)
}

func plugin_reload_controller(w http.ResponseWriter, req *http.Request) {
	err := plugin.LoadPlugin()
	if err != nil {
		w.Write(returnResult(false, err.Error()))
	} else {
		w.Write(returnResult(true, "success"))
	}
}
