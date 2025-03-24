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
	"html/template"
	"io/ioutil"
)

type ChannelController struct {
	CommonController
}

type ChannelParam struct {
	DbName       string
	ChannelId    int
	ChannelName  string
	CosumerCount int
}

func (c *ChannelController) getParam() *ChannelParam {
	body, err := ioutil.ReadAll(c.Ctx.Request.Body)
	if err != nil {
		result := ResultDataStruct{Status: 0, Msg: err.Error(), Data: nil}
		c.SetJsonData(result)
		c.StopServeJSON()
		return nil
	}
	var data ChannelParam
	if err = json.Unmarshal(body, &data); err != nil {
		result := ResultDataStruct{Status: 0, Msg: err.Error(), Data: nil}
		c.SetJsonData(result)
		c.StopServeJSON()
		return nil
	}
	return &data
}

func (c *ChannelController) Index() {
	DbName := c.Ctx.Request.Form.Get("DbName")
	c.SetData("Title", "Channel")
	c.SetData("ChannelList", server.GetDBObj(DbName).ListChannel())
	c.SetData("DbName", DbName)
	op := make([]string, 0)
	op = append(op, "/template/channel.list.html")
	template.ParseFiles(op...)
	c.AddAdminTemplate("channel.list.html", "header.html", "footer.html")
}

func (c *ChannelController) List() {
	DbName := c.Ctx.Request.Form.Get("DbName")
	c.SetJsonData(server.GetDBObj(DbName).ListChannel())
	c.StopServeJSON()
}

func (c *ChannelController) Add() {
	param := c.getParam()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	db := server.GetDBObj(param.DbName)
	if db == nil {
		result.Msg = param.DbName + " not exsit"
		return
	}
	_, ChannelID := db.AddChannel(param.ChannelName, param.CosumerCount)
	defer server.SaveDBConfigInfo()
	result = ResultDataStruct{Status: 1, Msg: "success", Data: ChannelID}
}

func (c *ChannelController) Stop() {
	param := c.getParam()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	ch := server.GetChannel(param.DbName, param.ChannelId)
	if ch == nil {
		result.Msg = param.DbName + " channelId:" + fmt.Sprint(param.ChannelId) + " not exsit"
		return
	}
	ch.Stop()
	defer server.SaveDBConfigInfo()
	result = ResultDataStruct{Status: 1, Msg: "success", Data: param.ChannelId}
}

func (c *ChannelController) Close() {
	param := c.getParam()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	ch := server.GetChannel(param.DbName, param.ChannelId)
	if ch == nil {
		result.Msg = param.DbName + " channelId:" + fmt.Sprint(param.ChannelId) + " not exsit"
		return
	}
	ch.Close()
	defer server.SaveDBConfigInfo()
	result = ResultDataStruct{Status: 1, Msg: "success", Data: param.ChannelId}
}

func (c *ChannelController) Start() {
	param := c.getParam()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	ch := server.GetChannel(param.DbName, param.ChannelId)
	if ch == nil {
		result.Msg = param.DbName + " channelId:" + fmt.Sprint(param.ChannelId) + " not exsit"
		return
	}
	ch.Start()
	defer server.SaveDBConfigInfo()
	result = ResultDataStruct{Status: 1, Msg: "success", Data: param.ChannelId}
}

func (c *ChannelController) Delete() {
	param := c.getParam()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	db := server.GetDBObj(param.DbName)
	TableMap := db.GetTableByChannelKey(param.DbName, param.ChannelId)
	n := len(TableMap)
	if len(TableMap) > 0 {
		result.Msg = "The channel bind table count:" + fmt.Sprint(n)
		return
	}
	r := server.DelChannel(param.DbName, param.ChannelId)
	if r == true {
		defer server.SaveDBConfigInfo()
		result = ResultDataStruct{Status: 1, Msg: "success", Data: param.ChannelId}
	} else {
		result.Msg = "channel or db not exsit"
	}
	return
}

func (c *ChannelController) TableListIndex() {
	DbName := c.Ctx.Request.Form.Get("DbName")
	channelID, _ := c.Ctx.GetParamInt64("ChannelId", 0)
	channelInfo := server.GetChannel(DbName, int(channelID))
	if channelInfo == nil {
		c.SetJsonData(ResultDataStruct{Status: 0, Msg: "channel not exsit", Data: nil})
		c.StopServeJSON()
		return
	}
	db := server.GetDBObj(DbName)
	TableMap := db.GetTableByChannelKey(DbName, int(channelID))
	c.SetData("TableList", TableMap)
	c.SetData("DbName", DbName)
	c.SetData("ChannelName", channelInfo.Name)
	c.SetData("ChannelID", channelID)
	c.SetData("Title", DbName+" - Table List - Channel")
	c.AddAdminTemplate("channel.table.list.html", "header.html", "footer.html")
}

func (c *ChannelController) TableList() {
	DbName := c.Ctx.Request.Form.Get("DbName")
	channelID, _ := c.Ctx.GetParamInt64("ChannelId", 0)
	channelInfo := server.GetChannel(DbName, int(channelID))
	if channelInfo == nil {
		c.SetJsonData(ResultDataStruct{Status: 0, Msg: "channel not exsit", Data: nil})
		c.StopServeJSON()
		return
	}
	db := server.GetDBObj(DbName)
	TableMap := db.GetTableByChannelKey(DbName, int(channelID))
	c.SetJsonData(TableMap)
	c.StopServeJSON()
}
