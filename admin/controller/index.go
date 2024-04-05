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
	"fmt"
	"github.com/brokercap/Bifrost/config"
	"github.com/brokercap/Bifrost/plugin/driver"
	pluginStorage "github.com/brokercap/Bifrost/plugin/storage"
	"github.com/brokercap/Bifrost/server"
	"io"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

var StartTime = ""

func init() {
	StartTime = time.Now().Format("2006-01-02 15:04:05")
}

type IndexController struct {
	CommonController
}

// 首页
func (c *IndexController) Index() {
	c.SetTitle("Index")
	c.SetData("ServerStartTime", server.GetServerStartTime().Format("2006-01-02"))
	c.AddAdminTemplate("index.html", "header.html", "footer.html")
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
	type MemStat struct {
		ResUsed uint64
		Runtime *runtime.MemStats
	}
	memStat := MemStat{
		Runtime: new(runtime.MemStats),
	}
	runtime.ReadMemStats(memStat.Runtime)
	memStat.ResUsed = c.getMemResUsed()
	c.SetJsonData(memStat)
	c.StopServeJSON()
}

// 获取当前进程相对应Top命令出来的结果值中相对应的RES列的值
// RES对应的值，对于真正内存使用理，相对更为准确合理
// runtime.MemStats 中是由Go自行统计的，可能存在误差
func (c *IndexController) getMemResUsed() uint64 {
	if runtime.GOOS != "linux" {
		return 0
	}

	statusFilePath := fmt.Sprintf("/proc/%d/status", os.Getpid())

	f, err := os.Open(statusFilePath)
	if err != nil {
		return 0
	}
	defer f.Close()
	content, err := io.ReadAll(f)
	if err != nil {
		return 0
	}

	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				// 解析VmRSS值为uint64
				vmRSSValue, parseErr := strconv.ParseUint(fields[1], 10, 64)
				if parseErr != nil {
					return 0
				}
				return vmRSSValue * 1024
			}
		}
	}
	return 0
}

// 强制运行 golang gc
func (c *IndexController) FreeOSMemory() {
	debug.FreeOSMemory()
	result := ResultDataStruct{Status: 1, Msg: "success", Data: nil}
	c.SetJsonData(result)
}
