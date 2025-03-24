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
	"net/http"
	"strings"

	"github.com/brokercap/Bifrost/admin/xgo"
	"github.com/brokercap/Bifrost/config"
	"github.com/brokercap/Bifrost/server/user"
)

type CommonController struct {
	xgo.Controller
}

var writeRequestOp = []string{"/add", "/del", "/start", "/stop", "/close", "/deal", "/update", "/export", "/import", "kill"}
var skipCheckAuthUriMap = map[string]bool{
	"/login/index": true,
	"/dologin":     true,
	"/logout":      true,
}

// 判断是否为写操作
func (c *CommonController) checkWriteRequest(uri string) bool {
	for _, v := range writeRequestOp {
		if strings.Contains(uri, v) {
			return true
		}
	}
	return false
}

func (c *CommonController) Prepare() {
	var ok bool
	if c.Ctx.Request.Header.Get("Authorization") != "" {
		ok = c.basicAuthor()
	} else {
		ok = c.normalAuthor()
		c.Data["Version"] = config.VERSION
	}
	if !ok {
		c.authErrExit()
	}
}

func (c *CommonController) checkAdminWriteRequest(group string) bool {
	if group != "administrator" && c.checkWriteRequest(c.Ctx.Request.RequestURI) {
		c.SetJsonData(ResultDataStruct{Status: -1, Msg: "user group : [ " + group + " ] no authority", Data: nil})
		c.StopServeJSON()
		return false
	}
	return true
}

func (c *CommonController) authErrExit() {
	c.SetJsonData(ResultDataStruct{Status: -1, Msg: "Author error", Data: nil})
	c.StopServeJSON()
}

func (c *CommonController) basicAuthor() bool {
	UserName, Password, ok := c.Ctx.Request.BasicAuth()
	if !ok || UserName == "" {
		c.authErrExit()
		return false
	}
	mayXRealIP, remoteAddrIp := c.GetRemoteIp()
	userInfo, err := user.CheckUserWithIP(UserName, Password, mayXRealIP, remoteAddrIp)
	if err != nil {
		c.SetJsonData(ResultDataStruct{Status: -1, Msg: err.Error(), Data: nil})
		c.StopServeJSON()
		return false
	}
	return c.checkAdminWriteRequest(userInfo.Group)
}

func (c *CommonController) normalAuthor() bool {
	var sessionID = c.Ctx.Session.CheckCookieValid(c.Ctx.ResponseWriter, c.Ctx.Request)
	if sessionID != "" {
		if _, ok := c.Ctx.Session.GetSessionVal(sessionID, "UserName"); ok {
			//非administrator用户 用户，没有写操作权限
			Group, _ := c.Ctx.Session.GetSessionVal(sessionID, "Group")
			return c.checkAdminWriteRequest(Group.(string))
		} else {
			goto toLogin
		}
	} else {
		goto toLogin
	}

toLogin:
	if _, ok := skipCheckAuthUriMap[c.Ctx.Request.RequestURI]; !ok {
		if c.IsHtmlOutput() {
			http.Redirect(c.Ctx.ResponseWriter, c.Ctx.Request, "/login/index", http.StatusFound)
			return false
		}
		c.SetJsonData(ResultDataStruct{Status: -1, Msg: "session time out", Data: nil})
		c.StopServeJSON()
		return false
	}
	return true
}

func (c *CommonController) SetTitle(title string) {
	c.SetData("Title", title+" - Bifrost")
}

func (c *CommonController) AddAdminTemplate(tpl ...string) {
	for _, tplName := range tpl {
		c.AddTemplate(AdminTemplatePath("/template/" + tplName))
	}
}

func (c *CommonController) AddPluginTemplate(tpl ...string) {
	for _, tplName := range tpl {
		c.AddTemplate(PluginTemplatePath("/plugin/" + tplName))
	}
}

func (c *CommonController) GetRemoteIp() (mayXRealIP, remoteAddrIp string) {
	// 这里也可以通过X-Forwarded-For请求头的第一个值作为用户的ip
	// 但是要注意的是这两个请求头代表的ip都有可能是伪造的
	mayXRealIP = c.Ctx.Request.Header.Get("X-Real-IP")
	remoteAddrIp = strings.Split(c.Ctx.Request.RemoteAddr, ":")[0]
	if mayXRealIP == "" {
		// 当请求头不存在即不存在代理时直接获取ip
		mayXRealIP = remoteAddrIp
	}
	return mayXRealIP, remoteAddrIp
}
