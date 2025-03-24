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
	"fmt"
	inputDriver "github.com/brokercap/Bifrost/input/driver"
	"github.com/brokercap/Bifrost/server"
	"github.com/brokercap/Bifrost/server/history"
	"io/ioutil"
	"strings"
)

type HistoryController struct {
	CommonController
}

type HistoryParam struct {
	DbName      string
	SchemaName  string
	TableName   string
	TableNames  string
	Property    history.HistoryProperty
	ToserverIds []int
	Id          int
	Crontab     string // 定时任务表达式
}

func (c *HistoryController) getParam() *HistoryParam {
	body, err := ioutil.ReadAll(c.Ctx.Request.Body)
	if err != nil {
		result := ResultDataStruct{Status: 0, Msg: err.Error(), Data: nil}
		c.SetJsonData(result)
		c.StopServeJSON()
		return nil
	}
	var data HistoryParam
	if err = json.Unmarshal(body, &data); err != nil {
		result := ResultDataStruct{Status: 0, Msg: err.Error(), Data: nil}
		c.SetJsonData(result)
		c.StopServeJSON()
		return nil
	}
	return &data
}

func (c *HistoryController) Index() {
	DbName := c.Ctx.Request.Form.Get("DbName")
	TableName := c.Ctx.Request.Form.Get("TableName")
	SchemaName := c.Ctx.Request.Form.Get("SchemaName")
	var status history.HisotryStatus
	switch c.Ctx.Request.Form.Get("Status") {
	case "close":
		status = history.HISTORY_STATUS_CLOSE
		break
	case "running":
		status = history.HISTORY_STATUS_RUNNING
		break
	case "selectOver":
		status = history.HISTORY_STATUS_SELECT_OVER
		break
	case "over":
		status = history.HISTORY_STATUS_OVER
		break
	case "halfway":
		status = history.HISTORY_STATUS_HALFWAY
		break
	case "killed":
		status = history.HISTORY_STATUS_KILLED
		break
	case "stoping":
		status = history.HISTORY_STATUS_SELECT_STOPING
		break
	default:
		status = history.HISTORY_STATUS_ALL
		break
	}
	HistoryList := history.GetHistoryList(DbName, SchemaName, tansferTableName(TableName), status)

	StatusList := []history.HisotryStatus{
		history.HISTORY_STATUS_ALL,
		history.HISTORY_STATUS_CLOSE,
		history.HISTORY_STATUS_RUNNING,
		history.HISTORY_STATUS_SELECT_STOPING,
		history.HISTORY_STATUS_SELECT_STOPED,
		history.HISTORY_STATUS_HALFWAY,
		history.HISTORY_STATUS_SELECT_OVER,
		history.HISTORY_STATUS_OVER,
		history.HISTORY_STATUS_KILLED,
	}
	c.SetData("DbName", DbName)
	c.SetData("TableName", TableName)
	c.SetData("SchemaName", SchemaName)
	c.SetData("HistoryList", HistoryList)
	c.SetData("DbList", server.GetListDb())
	c.SetData("Status", status)
	c.SetData("StatusList", StatusList)
	c.SetData("DbName", DbName)

	c.SetTitle("History List")
	c.AddAdminTemplate("history.list.html", "header.html", "footer.html")
}

func (c *HistoryController) List() {
	DbName := c.Ctx.Request.Form.Get("DbName")
	TableName := c.Ctx.Request.Form.Get("TableName")
	SchemaName := c.Ctx.Request.Form.Get("SchemaName")
	var status history.HisotryStatus
	switch c.Ctx.Request.Form.Get("Status") {
	case "close":
		status = history.HISTORY_STATUS_CLOSE
		break
	case "running":
		status = history.HISTORY_STATUS_RUNNING
		break
	case "selectOver":
		status = history.HISTORY_STATUS_SELECT_OVER
		break
	case "over":
		status = history.HISTORY_STATUS_OVER
		break
	case "halfway":
		status = history.HISTORY_STATUS_HALFWAY
		break
	case "killed":
		status = history.HISTORY_STATUS_KILLED
		break
	case "stoping":
		status = history.HISTORY_STATUS_SELECT_STOPING
		break
	default:
		status = history.HISTORY_STATUS_ALL
		break
	}
	HistoryList := history.GetHistoryList(DbName, SchemaName, tansferTableName(TableName), status)
	c.SetJsonData(HistoryList)
	c.StopServeJSON()
}

func (c *HistoryController) Add() {
	param := c.getParam()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	db := server.GetDbInfo(param.DbName)
	if db == nil {
		result.Msg = fmt.Sprintf("DbName: %s not esxit", param.DbName)
		return
	}
	o := inputDriver.Open(db.InputType, inputDriver.InputInfo{})
	if o == nil {
		result.Msg = fmt.Sprintf("DbName: %s Input: %s not esxit", db.Name, db.InputType)
		return
	}
	if !o.IsSupported(inputDriver.SupportFull) {
		result.Msg = fmt.Sprintf("DbName: %s Input: %s Full is not supported", db.Name, db.InputType)
		return
	}
	if tansferTableName(param.SchemaName) == "*" {
		result.Msg = "不能给 AllDataBases 添加全量任务!"
		return
	}
	if param.TableNames == "" {
		result.Msg = "table_names not be empty!"
		return
	}
	if len(param.ToserverIds) == 0 {
		result.Msg = "ToserverIds error!"
		return
	}
	tablenameArr := strings.Split(param.TableNames, ";")
	tableNameTest := ""
	for _, v := range tablenameArr {
		if v != "" {
			tableNameTest = v
			break
		}
	}
	if tableNameTest == "" {
		result.Msg = "table_names error!"
		return
	}
	var err error
	err = history.CheckWhere(param.DbName, param.SchemaName, tableNameTest, param.Property.Where)
	if err != nil {
		result.Msg = err.Error()
		return
	}

	var ID int
	ID, err = history.AddHistory(param.DbName, param.SchemaName, tansferTableName(param.TableName), param.TableNames, param.Property, param.ToserverIds)
	if err != nil {
		result.Msg = err.Error()
	} else {
		result = ResultDataStruct{Status: 1, Msg: "success", Data: ID}
	}
}

func (c *HistoryController) Delete() {
	param := c.getParam()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	b := history.DelHistory(param.DbName, param.Id)
	if b == false {
		result.Msg = "delete error"
	} else {
		result = ResultDataStruct{Status: 1, Msg: "success", Data: param.Id}
	}
}

func (c *HistoryController) Start() {
	param := c.getParam()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	err := history.Start(param.DbName, param.Id)
	if err != nil {
		result.Msg = err.Error()
	} else {
		result = ResultDataStruct{Status: 1, Msg: "success", Data: param.Id}
	}
}

func (c *HistoryController) Kill() {
	param := c.getParam()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	err := history.KillHistory(param.DbName, param.Id)
	if err != nil {
		result.Msg = err.Error()
	} else {
		result = ResultDataStruct{Status: 1, Msg: "success", Data: param.Id}
	}
}

func (c *HistoryController) Stop() {
	param := c.getParam()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	err := history.StopHistory(param.DbName, param.Id)
	if err != nil {
		result.Msg = err.Error()
	} else {
		result = ResultDataStruct{Status: 1, Msg: "success", Data: param.Id}
	}
}

func (c *HistoryController) CheckWhere() {
	param := c.getParam()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	var err error
	if param.Property.Where == "" {
		result = ResultDataStruct{Status: 1, Msg: "success", Data: param.Id}
		return
	}
	err = history.CheckWhere(param.DbName, param.SchemaName, param.TableName, param.Property.Where)
	if err != nil {
		result.Msg = err.Error()
		return
	}
	result = ResultDataStruct{Status: 1, Msg: "success", Data: 0}
}
