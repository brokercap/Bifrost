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
	"github.com/brokercap/Bifrost/plugin/driver"
	"net/http"
	"text/template"
)

func init() {
	addRoute("/docs", docs_controller)
}

func docs_controller(w http.ResponseWriter, req *http.Request) {
	type docs struct {
		TemplateHeader
		PluginKey string
		Drivers   map[string]driver.DriverStructure
	}
	req.ParseForm()
	PluginKey := req.Form.Get("plugin")
	data := docs{
		PluginKey: PluginKey,
		Drivers:   driver.Drivers(),
	}
	data.Title = "docs - Bifrost"
	t, _ := template.ParseFiles(TemplatePath("/manager/template/docs.html"), TemplatePath("/manager/template/header.html"), TemplatePath("/manager/template/footer.html"))
	t.Execute(w, data)
}
