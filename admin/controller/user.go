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
	"github.com/brokercap/Bifrost/server/user"
	"io/ioutil"
	"strings"
)

type UserController struct {
	CommonController
}

type UserParam struct {
	UserName string
	Password string
	Group    string
	Host     string
}

func (c *UserController) getParam() *UserParam {
	body, err := ioutil.ReadAll(c.Ctx.Request.Body)
	if err != nil {
		result := ResultDataStruct{Status: 0, Msg: err.Error(), Data: nil}
		c.SetJsonData(result)
		c.StopServeJSON()
		return nil
	}
	var data UserParam
	if err = json.Unmarshal(body, &data); err != nil {
		result := ResultDataStruct{Status: 0, Msg: err.Error(), Data: nil}
		c.SetJsonData(result)
		c.StopServeJSON()
		return nil
	}
	return &data
}

func (c *UserController) Index() {
	UserList := user.GetUserList()
	//过滤密码,防止其他 monitor 用户查看到
	for k, _ := range UserList {
		UserList[k].Password = ""
	}
	c.SetData("UserList", UserList)
	c.SetTitle("UserList")
	c.AddAdminTemplate("user.list.html", "header.html", "footer.html")
}

func (c *UserController) List() {
	UserList := user.GetUserList()
	//过滤密码,防止其他 monitor 用户查看到
	for k, _ := range UserList {
		UserList[k].Password = ""
	}
	c.SetJsonData(UserList)
	c.StopServeJSON()
}

func (c *UserController) Update() {
	param := c.getParam()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	if param.UserName == "" || param.Password == "" {
		result.Msg = " user_name and password not empty!"
		return
	}
	for _, Host := range strings.Split(param.Host, ",") {
		if strings.Count(Host, ".") > 3 {
			result.Msg = " Host error!"
			return
		}
	}
	err := user.UpdateUser(param.UserName, param.Password, param.Group, param.Host)
	if err != nil {
		result.Msg = err.Error()
	} else {
		result = ResultDataStruct{Status: 1, Msg: "success", Data: nil}
	}
	return
}

func (c *UserController) Delete() {
	param := c.getParam()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	if param.UserName == "" {
		result.Msg = " user_name not empty!"
		return
	}
	err := user.DelUser(param.UserName)
	if err != nil {
		result.Msg = err.Error()
	} else {
		result = ResultDataStruct{Status: 1, Msg: "success", Data: nil}
	}
	return
}

func (c *UserController) LastLoginLog() {
	result := ResultDataStruct{Status: 1, Msg: "success", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	logInfo, err := user.GetLastLoginLog()
	if err != nil {
		result.Data = err.Error()
	} else {
		result.Data = logInfo
	}
}
