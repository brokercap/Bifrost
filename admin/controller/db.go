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
	"github.com/brokercap/Bifrost/server"
	"io/ioutil"
	"log"
	"runtime/debug"
	"time"
	"github.com/brokercap/Bifrost/Bristol/mysql"
)

type DBController struct {
	CommonController
}

type DbUpdateParam struct {
	DbName            string
	SchemaName 		  string
	TableName         string
	Uri               string
	BinlogFileName    string
	BinlogPosition    uint32
	ServerId          uint32
	MaxBinlogFileName string
	MaxBinlogPosition uint32
	UpdateToServer    int8
	CheckPrivilege	  bool
	Gtid			  string
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
	return &data
}

// 数据源列表，界面显示
func (c *DBController) Index() {
	dbList := server.GetListDb()
	c.SetData("Title", "db list")
	c.SetData("DBList", dbList)
	c.AddAdminTemplate("db.list.html","header.html","footer.html")
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
	if data.DbName == "" || data.Uri == "" || data.BinlogFileName == "" || data.BinlogPosition <= 0 || data.ServerId <= 0 {
		result.Msg = " param error!"
		return
	}
	if data.Gtid != "" {
		err := mysql.CheckGtid(data.Gtid)
		if err != nil {
			result.Msg = err.Error()
			return
		}
	}
	defer server.SaveDBConfigInfo()
	server.AddNewDB(data.DbName, data.Uri, data.Gtid, data.BinlogFileName, data.BinlogPosition, data.ServerId, data.MaxBinlogFileName, data.MaxBinlogPosition, time.Now().Unix())
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
	if data.DbName == "" || data.Uri == "" || data.BinlogFileName == "" || data.BinlogPosition <= 0 || data.ServerId <= 0 {
		result.Msg = " param error!"
		return
	}
	if data.Gtid != "" {
		err := mysql.CheckGtid(data.Gtid)
		if err != nil {
			result.Msg = err.Error()
			return
		}
	}
	err := server.UpdateDB(data.DbName, data.Uri, data.Gtid, data.BinlogFileName, data.BinlogPosition, data.ServerId, data.MaxBinlogFileName, data.MaxBinlogPosition, time.Now().Unix(), data.UpdateToServer)
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
	server.DbList[data.DbName].Stop()
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
	server.DbList[data.DbName].Start()
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
	server.DbList[data.DbName].Close()
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
	type dbInfoStruct struct {
		BinlogFile     string
		BinlogPosition int
		Gtid		   string
		ServerId       int
		BinlogFormat   string
		BinlogRowImage string
	}
	dbInfo := &dbInfoStruct{}
	var checkFun = func() (e error) {
		e = nil
		defer func() {
			if err := recover(); err != nil {
				log.Println(string(debug.Stack()))
				e = fmt.Errorf(fmt.Sprint(err))
				return
			}
		}()
		dbconn := DBConnect(data.Uri)
		if dbconn != nil {
			e = nil
		} else {
			e = fmt.Errorf("db conn ,uknow error;请排查 Bifrost 机器 到 MySQL 机器网络是否正常，防火墙是否开放等！")
		}
		if e != nil {
			return
		}
		defer dbconn.Close()
		if data.CheckPrivilege {
			e = CheckUserSlavePrivilege(dbconn)
			if e != nil {
				return
			}
		}
		MasterBinlogInfo := GetBinLogInfo(dbconn)
		if MasterBinlogInfo.File != "" {
			dbInfo.BinlogFile = MasterBinlogInfo.File
			dbInfo.BinlogPosition = MasterBinlogInfo.Position
			dbInfo.Gtid = MasterBinlogInfo.Executed_Gtid_Set
			dbInfo.ServerId = GetServerId(dbconn)
			variablesMap := GetVariables(dbconn, "binlog_format")
			BinlogRowImageMap := GetVariables(dbconn, "binlog_row_image")
			if _, ok := variablesMap["binlog_format"]; ok {
				dbInfo.BinlogFormat = variablesMap["binlog_format"]
			}
			if _, ok := BinlogRowImageMap["binlog_row_image"]; ok {
				dbInfo.BinlogRowImage = BinlogRowImageMap["binlog_row_image"]
			}
		} else {
			e = fmt.Errorf("The binlog maybe not open,or no replication client privilege(s).you can show log more.")
		}
		return
	}

	err := checkFun()

	if err != nil {
		result.Msg = err.Error()
	} else {
		result = ResultDataStruct{Status: 1, Msg: "success", Data: dbInfo}
	}
	return
}

// 验证连接配置是否有效
func (c *DBController) GetLastPosition() {
	type dbInfoStruct struct {
		BinlogFile            string
		BinlogPosition        int
		BinlogTimestamp       uint32
		Gtid			      string
		CurrentBinlogFile     string
		CurrentBinlogPosition int
		CurrentGtid			  string
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
	dbInfo := &dbInfoStruct{ NowTimestamp: uint32(time.Now().Unix()) }
	dbInfo.BinlogFile = dbObj.BinlogDumpFileName
	dbInfo.BinlogPosition = int(dbObj.BinlogDumpPosition)
	dbInfo.BinlogTimestamp = dbObj.BinlogDumpTimestamp
	dbInfo.Gtid			  = dbObj.Gtid
	var f = func() (e error) {
		e = nil
		defer func() {
			if err := recover(); err != nil {
				log.Println(err)
				log.Println(string(debug.Stack()))
				e = fmt.Errorf(fmt.Sprint(err))
				return
			}
		}()
		dbconn := DBConnect(dbObj.ConnectUri)
		if dbconn != nil {
			e = nil
		} else {
			e = fmt.Errorf("db conn ,uknow error;请排查 Bifrost 机器 到 MySQL 机器网络是否正常，防火墙是否开放等！")
		}
		defer dbconn.Close()
		MasterBinlogInfo := GetBinLogInfo(dbconn)
		if MasterBinlogInfo.File != "" {
			dbInfo.CurrentBinlogFile = MasterBinlogInfo.File
			dbInfo.CurrentBinlogPosition = MasterBinlogInfo.Position
			dbInfo.CurrentGtid = MasterBinlogInfo.Executed_Gtid_Set
			if dbInfo.BinlogTimestamp > 0 && dbInfo.CurrentBinlogFile != dbInfo.BinlogFile &&  dbInfo.CurrentBinlogPosition != dbInfo.BinlogPosition {
				dbInfo.DelayedTime = dbInfo.NowTimestamp - dbInfo.BinlogTimestamp
			}
		} else {
			e = fmt.Errorf("The binlog maybe not open,or no replication client privilege(s).you can show log more.")
		}
		return
	}
	err := f()
	if err != nil {
		result.Msg = err.Error()
	} else {
		result = ResultDataStruct{Status: 1, Msg: "success", Data: dbInfo}
	}
	return
}
