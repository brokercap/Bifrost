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
	"github.com/brokercap/Bifrost/config"
	"github.com/brokercap/Bifrost/plugin/driver"
	pluginStorage "github.com/brokercap/Bifrost/plugin/storage"
	"github.com/brokercap/Bifrost/server"
	"runtime"
	"time"
	"runtime/debug"
)

var StartTime = ""

func init() {
	StartTime = time.Now().Format("2006-01-03 15:04:05")
}

type IndexController struct {
	CommonController
}

// 首页
func (c *IndexController) Index() {
	c.SetTitle("Index")
	c.AddAdminTemplate("index.html","header.html","footer.html")
}

// Bifrostd 基本信息
func (c *IndexController) Overview() {
	dbList := server.GetListDb()
	DbCount := len(dbList)

	TableCount := 0
	for _, v := range dbList {
		TableCount += v.TableCount
	}

	PluginCount := len(driver.Drivers())

	ToServerCount := len(pluginStorage.GetToServerMap())

	c.SetData("DbCount", DbCount)
	c.SetData("ToServerCount", ToServerCount)
	c.SetData("PluginCount", PluginCount)
	c.SetData("TableCount", TableCount)
	c.SetData("GoVersion", runtime.Version())
	c.SetData("BifrostVersion", config.VERSION)
	c.SetData("BifrostPluginVersion", driver.GetApiVersion())
	c.SetData("StartTime", StartTime)
	c.SetData("GOOS", runtime.GOOS)
	c.SetData("GOARCH", runtime.GOARCH)
	c.StopServeJSON()
}

// 获取 golang 运行的基本信息
func (c *IndexController) ServerMonitor() {
	memStat := new(runtime.MemStats)
	runtime.ReadMemStats(memStat)
	c.SetJsonData(memStat)
	c.StopServeJSON()
}

// 强制运行 golang gc
func (c *IndexController) FreeOSMemory() {
	debug.FreeOSMemory()
	result := ResultDataStruct{Status: 1, Msg: "success", Data: nil}
	c.SetJsonData(result)
}
