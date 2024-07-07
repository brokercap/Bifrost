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

type TableController struct {
	CommonController
}

type TableParam struct {
	DbName      string
	SchemaName  string
	TableName   string
	IgnoreTable string
	DoTable     string
	ChannelId   int
	Id          int
}

func (c *TableController) getParam() *TableParam {
	body, err := ioutil.ReadAll(c.Ctx.Request.Body)
	if err != nil {
		result := ResultDataStruct{Status: 0, Msg: err.Error(), Data: nil}
		c.SetJsonData(result)
		c.StopServeJSON()
		return nil
	}
	var data TableParam
	if err = json.Unmarshal(body, &data); err != nil {
		result := ResultDataStruct{Status: 0, Msg: err.Error(), Data: nil}
		c.SetJsonData(result)
		c.StopServeJSON()
		return nil
	}
	return &data
}

func (c *TableController) Add() {
	param := c.getParam()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	SchemaName := tansferSchemaName(param.SchemaName)
	TableName := tansferTableName(param.TableName)
	err := server.AddTable(param.DbName, SchemaName, TableName, param.IgnoreTable, param.DoTable, param.ChannelId)
	if err != nil {
		result.Msg = err.Error()
		return
	}
	result = ResultDataStruct{Status: 1, Msg: "success", Data: param.Id}
}

func (c *TableController) Update() {
	param := c.getParam()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	SchemaName := tansferSchemaName(param.SchemaName)
	TableName := tansferTableName(param.TableName)
	err := server.UpdateTable(param.DbName, SchemaName, TableName, param.IgnoreTable, param.DoTable)
	if err != nil {
		result.Msg = err.Error()
		return
	}
	result = ResultDataStruct{Status: 1, Msg: "success", Data: param.Id}
}

func (c *TableController) Delete() {
	param := c.getParam()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	SchemaName := tansferSchemaName(param.SchemaName)
	TableName := tansferTableName(param.TableName)
	err := server.DelTable(param.DbName, SchemaName, TableName)
	if err != nil {
		result.Msg = err.Error()
		return
	}
	result = ResultDataStruct{Status: 1, Msg: "success", Data: param.Id}
}

func (c *TableController) List() {
	DbName := c.Ctx.Request.Form.Get("DbName")
	defer func() {
		c.StopServeJSON()
	}()
	DbName = tansferSchemaName(DbName)
	tablesMap := server.GetDBObj(DbName).GetTables()
	c.SetJsonData(tablesMap)
}
