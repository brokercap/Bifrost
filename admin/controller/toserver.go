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
	"github.com/brokercap/Bifrost/plugin/driver"
	ToServerStorage "github.com/brokercap/Bifrost/plugin/storage"
	"github.com/brokercap/Bifrost/server"
	"io/ioutil"
)

type ToServerController struct {
	CommonController
}

type ToServerParam struct {
	ToServerKey string
	PluginName  string
	Notes       string
	ConnUri     string
	MaxConn     int // 最大连接数
	MinConn     int // 最小连接数
}

func (c *ToServerController) getParam() *ToServerParam {
	body, err := ioutil.ReadAll(c.Ctx.Request.Body)
	if err != nil {
		result := ResultDataStruct{Status: 0, Msg: err.Error(), Data: nil}
		c.SetJsonData(result)
		c.StopServeJSON()
		return nil
	}
	var data ToServerParam
	if err = json.Unmarshal(body, &data); err != nil {
		result := ResultDataStruct{Status: 0, Msg: err.Error(), Data: nil}
		c.SetJsonData(result)
		c.StopServeJSON()
		return nil
	}
	return &data
}

func (c *ToServerController) Index() {
	c.SetData("ToServerList", ToServerStorage.ToServerMap)
	c.SetData("Drivers", driver.Drivers())
	c.SetTitle("ToServer List")
	c.AddAdminTemplate("toserver.list.html", "header.html", "footer.html")
}

func (c *ToServerController) List() {
	c.SetData("ToServerList", ToServerStorage.ToServerMap)
	c.SetData("Drivers", driver.Drivers())
	c.StopServeJSON()
}

func (c *ToServerController) CheckUri() {
	param := c.getParam()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	if param.PluginName == "" || param.ConnUri == "" {
		result.Msg = "PluginName,connuri muest be not empty"
		return
	}
	err := driver.CheckUri(param.PluginName, &param.ConnUri)
	if err != nil {
		result.Msg = err.Error()
		return
	}
	result = ResultDataStruct{Status: 1, Msg: "success", Data: nil}
}

func (c *ToServerController) Add() {
	param := c.getParam()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	if param.ToServerKey == "" || param.PluginName == "" || param.ConnUri == "" {
		result.Msg = "toserverkey,PluginName,connuri muest be not empty"
		return
	}
	ToServerStorage.SetToServerInfo(
		param.ToServerKey,
		ToServerStorage.ToServer{
			PluginName: param.PluginName,
			ConnUri:    param.ConnUri,
			Notes:      param.Notes,
			MaxConn:    param.MaxConn,
			MinConn:    param.MinConn,
		})
	defer server.SaveDBConfigInfo()
	result = ResultDataStruct{Status: 1, Msg: "success", Data: nil}
}

func (c *ToServerController) Update() {
	param := c.getParam()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	if param.ToServerKey == "" || param.PluginName == "" || param.ConnUri == "" {
		result.Msg = "toserverkey,PluginName,connuri muest be not empty"
		return
	}
	ToServerStorage.UpdateToServerInfo(
		param.ToServerKey,
		ToServerStorage.ToServer{
			PluginName: param.PluginName,
			ConnUri:    param.ConnUri,
			Notes:      param.Notes,
			MaxConn:    param.MaxConn,
			MinConn:    param.MinConn,
		})
	defer server.SaveDBConfigInfo()
	result = ResultDataStruct{Status: 1, Msg: "success", Data: nil}
}

func (c *ToServerController) Delete() {
	param := c.getParam()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	if param.ToServerKey == "" {
		result.Msg = "toserverkey muest be not empty"
		return
	}
	ToServerStorage.DelToServerInfo(param.ToServerKey)
	defer server.SaveDBConfigInfo()
	result = ResultDataStruct{Status: 1, Msg: "success", Data: nil}
}
