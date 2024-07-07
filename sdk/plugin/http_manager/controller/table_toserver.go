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
	"github.com/brokercap/Bifrost/admin/xgo"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	pluginStorage "github.com/brokercap/Bifrost/plugin/storage"
	"io/ioutil"
)

type TableToServerController struct {
	xgo.Controller
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
	t := make([]*pluginStorage.ToServer, 0)
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
	toServerInfo := pluginStorage.GetToServerInfo(param.ToServerKey)
	if toServerInfo == nil {
		result.Msg = param.ToServerKey + " not exsit"
		return
	}
	t := pluginDriver.Open(param.PluginName, &toServerInfo.ConnUri)
	if t == nil {
		result.Msg = "plugin new error"
		return
	}
	_, err := t.SetParam(param.PluginParam)
	if err != nil {
		result.Msg = err.Error()
		return
	}
	result = ResultDataStruct{Status: 1, Msg: "test success", Data: 1}
}
