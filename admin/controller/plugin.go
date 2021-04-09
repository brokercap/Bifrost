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
package controller

import (
	"github.com/brokercap/Bifrost/plugin"
	"github.com/brokercap/Bifrost/plugin/driver"
)

type PluginController struct {
	CommonController
}

func (c *PluginController) Index() {
	driversMap := driver.Drivers()
	//因为plugin 加载so 插件，有可能会异常，所以这里需要你把异常的插件列表也加载进来并进行显示出来
	errorPluginMap := plugin.GetErrorPluginList()
	for name, v := range errorPluginMap {
		driversMap[name] = v
	}
	c.SetTitle("Plugin List")
	c.SetData("PluginAPIVersion", driver.GetApiVersion())
	c.SetData("Drivers", driversMap)
	c.AddAdminTemplate("plugin.list.html", "header.html", "footer.html")
}

func (c *PluginController) List() {
	driversMap := driver.Drivers()
	//因为plugin 加载so 插件，有可能会异常，所以这里需要你把异常的插件列表也加载进来并进行显示出来
	errorPluginMap := plugin.GetErrorPluginList()
	for name, v := range errorPluginMap {
		driversMap[name] = v
	}
	c.SetTitle("Plugin List - Bifrost")
	c.SetData("PluginAPIVersion", driver.GetApiVersion())
	c.SetData("Drivers", driversMap)
}

func (c *PluginController) Reload() {
	err := plugin.LoadPlugin()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	if err != nil {
		result.Msg = err.Error()
	} else {
		result = ResultDataStruct{Status: 1, Msg: "success", Data: nil}
	}
}
