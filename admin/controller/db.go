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
	"io/ioutil"
	"strings"
	"time"

	"github.com/brokercap/Bifrost/Bristol/mysql"
	"github.com/brokercap/Bifrost/server"

	inputDriver "github.com/brokercap/Bifrost/input/driver"
)

type DBController struct {
	CommonController
}

type DbUpdateParam struct {
	DbName            string
	InputType         string
	SchemaName        string
	TableName         string
	Uri               string
	BinlogFileName    string
	BinlogPosition    uint32
	ServerId          uint32
	MaxBinlogFileName string
	MaxBinlogPosition uint32
	UpdateToServer    int8
	CheckPrivilege    bool
	Gtid              string
}

func (c *DBController) getParam() *DbUpdateParam {
	body, err := ioutil.ReadAll(c.Ctx.Request.Body)
	if err != nil {
		result := ResultDataStruct{Status: 0, Msg: err.Error(), Data: nil}
		c.SetJsonData(result)
		c.StopServeJSON()
		return nil
	}
	var data DbUpdateParam
	if err = json.Unmarshal(body, &data); err != nil {
		result := ResultDataStruct{Status: 0, Msg: err.Error(), Data: nil}
		c.SetJsonData(result)
		c.StopServeJSON()
		return nil
	}
	if data.InputType == "" {
		data.InputType = "mysql"
	}
	return &data
}

// 判断是否为mysql数据源
func (c *DBController) isMysqlInputType(data *DbUpdateParam) bool {
	return strings.Contains(strings.ToLower(data.InputType), "mysql")
}

// 数据源列表，界面显示
func (c *DBController) Index() {
	dbList := server.GetListDb()
	inputPluginsMap := inputDriver.Drivers()
	c.SetData("Title", "db list")
	c.SetData("DBList", dbList)
	c.SetData("inputPluginsMap", inputPluginsMap)
	c.AddAdminTemplate("db.list.html", "header.html", "footer.html")
}

// db list
func (c *DBController) List() {
	dbList := server.GetListDb()
	c.SetJsonData(dbList)
	c.StopServeJSON()
}

// add 数据源
func (c *DBController) Add() {
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	data := c.getParam()
	if data.DbName == "" || data.Uri == "" || data.BinlogFileName == "" || data.BinlogPosition < 0 || data.ServerId <= 0 {
		result.Msg = " param error!"
		return
	}
	if data.Gtid != "" && c.isMysqlInputType(data) {
		err := mysql.CheckGtid(data.Gtid)
		if err != nil {
			result.Msg = err.Error()
			return
		}
	}
	defer server.SaveDBConfigInfo()
	inputInfo := inputDriver.InputInfo{
		DbName:         data.DbName,
		ConnectUri:     data.Uri,
		GTID:           data.Gtid,
		BinlogFileName: data.BinlogFileName,
		BinlogPostion:  data.BinlogPosition,
		ServerId:       data.ServerId,
		MaxFileName:    data.MaxBinlogFileName,
		MaxPosition:    data.MaxBinlogPosition,
	}
	server.AddNewDB(data.DbName, data.InputType, inputInfo, time.Now().Unix())
	channel, _ := server.GetDBObj(data.DbName).AddChannel("default", 1)
	if channel != nil {
		channel.Start()
	}
	result = ResultDataStruct{Status: 1, Msg: "success", Data: nil}
}

// update 数据源
func (c *DBController) Update() {
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	data := c.getParam()
	if data.DbName == "" || data.Uri == "" || data.BinlogFileName == "" || data.BinlogPosition < 0 || data.ServerId <= 0 {
		result.Msg = " param error!"
		return
	}
	if data.Gtid != "" && c.isMysqlInputType(data) {
		err := mysql.CheckGtid(data.Gtid)
		if err != nil {
			result.Msg = err.Error()
			return
		}
	}
	inputInfo := inputDriver.InputInfo{
		DbName:         data.DbName,
		ConnectUri:     data.Uri,
		GTID:           data.Gtid,
		BinlogFileName: data.BinlogFileName,
		BinlogPostion:  data.BinlogPosition,
		ServerId:       data.ServerId,
		MaxFileName:    data.MaxBinlogFileName,
		MaxPosition:    data.MaxBinlogPosition,
	}
	err := server.UpdateDB(data.DbName, data.InputType, inputInfo, time.Now().Unix(), data.UpdateToServer)
	if err != nil {
		result.Msg = err.Error()
	} else {
		defer server.SaveDBConfigInfo()
		result = ResultDataStruct{Status: 1, Msg: "success", Data: nil}
	}
	return
}

// 删除数据源
func (c *DBController) Delete() {
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	data := c.getParam()
	if data.DbName == "" {
		result.Msg = " DbName not be empty!"
		return
	}
	defer server.SaveDBConfigInfo()
	r := server.DelDB(data.DbName)
	if r == true {
		result.Status = 1
		result.Msg = "success"
	} else {
		result.Msg = "error"
	}
	return
}

// 暂停数据源
func (c *DBController) Stop() {
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	data := c.getParam()
	if data.DbName == "" {
		result.Msg = " DbName not be empty!"
		return
	}
	defer server.SaveDBConfigInfo()
	server.GetDB(data.DbName).Stop()
	result = ResultDataStruct{Status: 1, Msg: "success", Data: nil}
	return
}

// 启动数据源
func (c *DBController) Start() {
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	data := c.getParam()
	if data.DbName == "" {
		result.Msg = " DbName not be empty!"
		return
	}
	defer server.SaveDBConfigInfo()
	err := server.GetDB(data.DbName).Start()
	if err != nil {
		result.Msg = err.Error()
		return
	}
	result = ResultDataStruct{Status: 1, Msg: "success", Data: nil}
	return
}

// 关闭数据源
func (c *DBController) Close() {
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	data := c.getParam()
	if data.DbName == "" {
		result.Msg = " DbName not be empty!"
		return
	}
	defer server.SaveDBConfigInfo()
	server.GetDB(data.DbName).Close()
	result = ResultDataStruct{Status: 1, Msg: "success", Data: nil}
	return
}

// 验证连接配置是否有效
func (c *DBController) CheckUri() {
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	data := c.getParam()
	if data.Uri == "" {
		result.Msg = " Uri not be empty!"
		return
	}
	inputInfo := inputDriver.InputInfo{
		DbName:         data.DbName,
		IsGTID:         false,
		ConnectUri:     data.Uri,
		GTID:           "",
		BinlogFileName: "",
		BinlogPostion:  0,
		ServerId:       0,
		MaxFileName:    "",
		MaxPosition:    0,
	}
	o := inputDriver.Open(data.InputType, inputInfo)
	CheckUriResult, err := o.CheckUri(data.CheckPrivilege)
	if err != nil {
		result.Msg = err.Error()
	} else {
		result = ResultDataStruct{Status: 1, Msg: "success", Data: CheckUriResult}
	}
	return
}

// 验证连接配置是否有效
func (c *DBController) GetLastPosition() {
	type dbInfoStruct struct {
		BinlogFile            string
		BinlogPosition        int
		BinlogTimestamp       uint32
		Gtid                  string
		CurrentBinlogFile     string
		CurrentBinlogPosition int
		CurrentGtid           string
		NowTimestamp          uint32
		DelayedTime           uint32
	}
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	data := c.getParam()
	if data.DbName == "" {
		result.Msg = "DbName not be empty!"
		return
	}
	dbObj := server.GetDbInfo(data.DbName)
	if dbObj == nil {
		result.Msg = data.DbName + " no exsit"
		return
	}
	dbInfo := &dbInfoStruct{NowTimestamp: uint32(time.Now().Unix())}
	dbInfo.BinlogFile = dbObj.BinlogDumpFileName
	dbInfo.BinlogPosition = int(dbObj.BinlogDumpPosition)
	dbInfo.BinlogTimestamp = dbObj.BinlogDumpTimestamp
	dbInfo.Gtid = dbObj.Gtid

	CurrentPositionInfo, err := server.GetDB(data.DbName).GetCurrentPosition()
	if err != nil {
		result.Msg = err.Error()
		return
	}
	if CurrentPositionInfo == nil {
		result.Msg = fmt.Sprintf("The binlog maybe not open,or no replication client privilege(s).you can show log more.")
		return
	}
	dbInfo.CurrentBinlogFile = CurrentPositionInfo.BinlogFileName
	dbInfo.CurrentBinlogPosition = int(CurrentPositionInfo.BinlogPostion)
	dbInfo.CurrentGtid = CurrentPositionInfo.GTID
	if dbInfo.BinlogTimestamp > 0 && dbInfo.CurrentBinlogFile != dbInfo.BinlogFile && dbInfo.CurrentBinlogPosition != dbInfo.BinlogPosition {
		dbInfo.DelayedTime = dbInfo.NowTimestamp - dbInfo.BinlogTimestamp
	}
	result = ResultDataStruct{Status: 1, Msg: "success", Data: dbInfo}
	return
}

// 获取mysql version
func (c *DBController) GetVersion() {
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	DbName := c.Ctx.Request.Form.Get("DbName")
	dbObj := server.GetDbInfo(DbName)
	if dbObj == nil {
		result.Msg = DbName + " no exsit"
		return
	}
	inputInfo := inputDriver.InputInfo{
		DbName:         DbName,
		IsGTID:         false,
		ConnectUri:     dbObj.ConnectUri,
		GTID:           "",
		BinlogFileName: "",
		BinlogPostion:  0,
		ServerId:       0,
		MaxFileName:    "",
		MaxPosition:    0,
	}
	o := inputDriver.Open(dbObj.InputType, inputInfo)
	Version, _ := o.GetVersion()
	result = ResultDataStruct{Status: 1, Msg: "success", Data: Version}
}
