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
	"github.com/brokercap/Bifrost/server/user"
	"net/http"
)

type LoginController struct {
	UserController
}

func (c *LoginController) Index() {
	c.SetTitle("Login")
	c.AddAdminTemplate("login.html")
}

func (c *LoginController) Login() {
	param := c.getParam()
	result := ResultDataStruct{Status: 0, Msg: "error", Data: nil}
	defer func() {
		c.SetJsonData(result)
		c.StopServeJSON()
	}()
	if param.UserName == "" {
		result.Msg = "user no exsit"
		return
	}
	var sessionID = c.Ctx.Session.StartSession(c.Ctx.ResponseWriter, c.Ctx.Request)
	UserInfo,err := user.CheckUserWithIP(param.UserName,param.Password,c.GetRemoteIp())
	if err == nil {
		c.Ctx.Session.SetSessionVal(sessionID, "UserName", param.UserName)
		c.Ctx.Session.SetSessionVal(sessionID, "Group", UserInfo.Group)
		result = ResultDataStruct{Status: 1, Msg: "success", Data: nil}
		return
	}
	result.Msg = err.Error()
	return
}

func (c *LoginController) Logout() {
	c.Ctx.Session.EndSession(c.Ctx.ResponseWriter, c.Ctx.Request)
	if c.IsHtmlOutput() {
		c.SetOutputByUser()
		http.Redirect(c.Ctx.ResponseWriter, c.Ctx.Request, "/login/index", http.StatusFound)
	}else{
		result := ResultDataStruct{Status: 1, Msg: "success", Data: nil}
		c.SetJsonData(result)
		c.StopServeJSON()
	}
}