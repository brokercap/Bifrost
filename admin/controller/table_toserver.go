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
	pluginStorage "github.com/brokercap/Bifrost/plugin/storage"
	"github.com/brokercap/Bifrost/server"
	"io/ioutil"
)

type TableToServerController struct {
	CommonController
}

type TableToServerParam struct {
	DbName        string
	SchemaName    string
	TableName     string
	ToServerKey   string
	PluginName    string
	FieldList     []string
	MustBeSuccess bool
	FilterQuery   bool
	FilterUpdate  bool
	PluginParam   map[string]interface{}
	ToServerId    int
	Index         int
}

func (c *TableToServerController) getParam() *TableToServerParam {
	body, err := ioutil.ReadAll(c.Ctx.Request.Body)
	if err != nil {
		result := ResultDataStruct{Status: 0, Msg: err.Error(), Data: nil}
		c.SetJsonData(result)
		c.StopServeJSON()
		return nil
	}
	var data TableToServerParam
	if err = json.Unmarshal(body, &data); err != nil {
		result := ResultDataStruct{Status: 0, Msg: err.Error(), Data: nil}
		c.SetJsonData(result)
		c.StopServeJSON()
		return nil
	}
	return &data
}

func (c *TableToServerController) List() {
	DbName := c.Ctx.Request.Form.Get("DbName")
	SchemaName := c.Ctx.Request.Form.Get("SchemaName")
	TableName := c.Ctx.Request.Form.Get("TableName")
	SchemaName = tansferSchemaName(SchemaName)
	TableName = tansferTableName(TableName)
	t1 := server.GetDBObj(DbName)
	t := t1.GetTable(SchemaName, TableName).ToServerList
	c.SetJsonData(t)
	c.StopServeJSON()
}

func (c *TableToServerController) Add() {
	param := c.getParam()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()

	if pluginStorage.GetToServerInfo(param.ToServerKey) == nil {
		result.Msg = param.ToServerKey + "not exsit"
		return
	}
	toServer := &server.ToServer{
		MustBeSuccess: param.MustBeSuccess,
		FilterQuery:   param.FilterQuery,
		FilterUpdate:  param.FilterUpdate,
		ToServerKey:   param.ToServerKey,
		PluginName:    param.PluginName,
		FieldList:     param.FieldList,
		PluginParam:   param.PluginParam,
	}
	SchemaName := tansferSchemaName(param.SchemaName)
	TableName := tansferTableName(param.TableName)
	dbObj := server.GetDBObj(param.DbName)
	r, ToServerId := dbObj.AddTableToServer(SchemaName, TableName, toServer)
	if r == true {
		defer server.SaveDBConfigInfo()
		result = ResultDataStruct{Status: 1, Msg: "success", Data: ToServerId}
	} else {
		result.Msg = "unkown error"
	}
}

func (c *TableToServerController) Delete() {
	param := c.getParam()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()

	SchemaName := tansferSchemaName(param.SchemaName)
	TableName := tansferTableName(param.TableName)

	server.GetDBObj(param.DbName).DelTableToServer(SchemaName, TableName, param.ToServerId)
	defer server.SaveDBConfigInfo()
	result = ResultDataStruct{Status: 1, Msg: "success", Data: param.ToServerId}
}

func (c *TableToServerController) DealError() {
	param := c.getParam()
	result := ResultDataStruct{Status: 1, Msg: "success", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()

	SchemaName := tansferSchemaName(param.SchemaName)
	TableName := tansferTableName(param.TableName)
	t := server.GetDBObj(param.DbName).GetTableSelf(SchemaName, TableName)
	ToServerInfo := t.ToServerList[param.Index]
	if ToServerInfo != nil && ToServerInfo.ToServerID == param.ToServerId {
		ToServerInfo.DealWaitError()
	}
}

func (c *TableToServerController) Stop() {
	param := c.getParam()
	result := ResultDataStruct{Status: 1, Msg: "success", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()

	SchemaName := tansferSchemaName(param.SchemaName)
	TableName := tansferTableName(param.TableName)
	t := server.GetDBObj(param.DbName).GetTableSelf(SchemaName, TableName)
	ToServerInfo := t.ToServerList[param.Index]
	if ToServerInfo != nil && ToServerInfo.ToServerID == param.ToServerId {
		ToServerInfo.Stop()
	}
}

func (c *TableToServerController) Start() {
	param := c.getParam()
	result := ResultDataStruct{Status: 1, Msg: "success", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()

	SchemaName := tansferSchemaName(param.SchemaName)
	TableName := tansferTableName(param.TableName)
	t := server.GetDBObj(param.DbName).GetTableSelf(SchemaName, TableName)
	ToServerInfo := t.ToServerList[param.Index]
	if ToServerInfo != nil && ToServerInfo.ToServerID == param.ToServerId {
		ToServerInfo.Start()
	}
}
