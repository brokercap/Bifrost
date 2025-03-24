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
	"github.com/brokercap/Bifrost/server"
	"io/ioutil"
	"time"
)

type BackupController struct {
	CommonController
}

// 导出配置
func (c *BackupController) Export() {
	b, err := server.GetSnapshotData()
	if err != nil {
		c.SetJsonData(ResultDataStruct{Status: 0, Msg: err.Error(), Data: nil})
		return
	}
	c.SetOutputByUser()
	fileName := "bifrost_" + time.Now().Format("2006-01-02 15:04:05") + ".json"
	c.Ctx.ResponseWriter.Header().Add("Content-Type", "application/octet-stream")
	c.Ctx.ResponseWriter.Header().Add("content-disposition", "attachment; filename=\""+fileName+"\"")
	c.Ctx.ResponseWriter.Write(b)
}

// 导入配置
func (c *BackupController) Import() {
	c.Ctx.Request.ParseMultipartForm(32 << 20)
	file, _, err := c.Ctx.Request.FormFile("backup_file")
	if err != nil {
		c.SetJsonData(ResultDataStruct{Status: 0, Msg: err.Error(), Data: nil})
		return
	}
	fileContent, err := ioutil.ReadAll(file)
	if err != nil {
		c.SetJsonData(ResultDataStruct{Status: 0, Msg: err.Error(), Data: nil})
		return
	}
	file.Close()
	server.DoRecoveryByBackupData(string(fileContent))
	c.SetJsonData(ResultDataStruct{Status: 1, Msg: "success", Data: nil})
}
