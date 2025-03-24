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
	"encoding/json"
	"github.com/brokercap/Bifrost/server"
	"io/ioutil"
)

type FileQueueController struct {
	CommonController
}

type TableFileQueueParam struct {
	DbName     string
	SchemaName string
	TableName  string
	ToServerId int
	Index      int
}

func (c *FileQueueController) getParam() *TableFileQueueParam {
	body, err := ioutil.ReadAll(c.Ctx.Request.Body)
	if err != nil {
		result := ResultDataStruct{Status: 0, Msg: err.Error(), Data: nil}
		c.SetJsonData(result)
		c.StopServeJSON()
		return nil
	}
	var data TableFileQueueParam
	if err = json.Unmarshal(body, &data); err != nil {
		result := ResultDataStruct{Status: 0, Msg: err.Error(), Data: nil}
		c.SetJsonData(result)
		c.StopServeJSON()
		return nil
	}
	return &data
}

func (c *FileQueueController) Update() {
	param := c.getParam()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	SchemaName := tansferSchemaName(param.SchemaName)
	TableName := tansferTableName(param.TableName)
	ToServerInfo := server.GetDBObj(param.DbName).GetTable(SchemaName, TableName).ToServerList[param.Index]
	if ToServerInfo.ToServerID != param.ToServerId {
		result.Msg = "ToServerID error"
		return
	}
	err := ToServerInfo.FileQueueStart()
	if err != nil {
		result.Msg = err.Error()
	} else {
		result = ResultDataStruct{Status: 1, Msg: "success", Data: param.ToServerId}
	}
}

func (c *FileQueueController) GetInfo() {
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	DbName := c.Ctx.Get("DbName")
	SchemaName := c.Ctx.Get("SchemaName")
	TableName := c.Ctx.Get("TableName")
	SchemaName = tansferSchemaName(SchemaName)
	TableName = tansferTableName(TableName)
	Index, _ := c.Ctx.GetParamInt64("Index", 0)
	ToServerId, _ := c.Ctx.GetParamInt64("ToServerId", -1)
	if DbName == "" || SchemaName == "" || TableName == "" || ToServerId < 0 {
		result.Msg = "param error!"
		return
	}
	ToServerInfo := server.GetDBObj(DbName).GetTable(SchemaName, TableName).ToServerList[int(Index)]
	if ToServerInfo.ToServerID != int(ToServerId) {
		result.Msg = "ToServerID error"
		return
	}
	FileInfo, err := ToServerInfo.GetFileQueueInfo()
	if err != nil {
		result.Msg = err.Error()
	} else {
		result = ResultDataStruct{Status: 1, Msg: "success", Data: FileInfo}
	}
}
