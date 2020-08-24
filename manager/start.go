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
package manager

import (
	"encoding/json"
	"github.com/brokercap/Bifrost/config"
	"github.com/brokercap/Bifrost/manager/xgo"
	"log"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
)

func TemplatePath(fileName string) string {
	return config.BifrostDir + fileName
}

type TemplateHeader struct {
	Title string
}

func (TemplateHeader *TemplateHeader) setTile(title string) {
	TemplateHeader.Title = title
}

type resultStruct struct {
	Status bool   `json:"status"`
	Msg    string `json:"msg"`
}

type resultDataStruct struct {
	Status bool        `json:"status"`
	Msg    string      `json:"msg"`
	Data   interface{} `json:"data"`
}

var sessionMgr *xgo.SessionMgr = nil //session管理器

func returnResult(r bool, msg string) []byte {
	b, _ := json.Marshal(resultStruct{Status: r, Msg: msg})
	return b
}

func returnDataResult(r bool, msg string, data interface{}) []byte {
	b, _ := json.Marshal(resultDataStruct{Status: r, Msg: msg, Data: data})
	return b
}

func GetFormInt(req *http.Request, key string) int {
	v := strings.Trim(req.Form.Get(key), "")
	intv, err := strconv.Atoi(v)
	if err != nil {
		return 0
	} else {
		return intv
	}
}

func addRoute(route string, callbackFUns func(http.ResponseWriter, *http.Request)) {
	xgo.AddRoute(route, callbackFUns)
}

var writeRequestOp = []string{"/add", "/del", "/start", "/stop", "/close", "/deal", "/update", "/export", "/import", "kill"}

//判断是否为写操作
func checkWriteRequest(uri string) bool {
	for _, v := range writeRequestOp {
		if strings.Contains(uri, v) {
			return true
		}
	}
	return false
}

func controller_FirstCallback(w http.ResponseWriter, req *http.Request) bool {
	if req.Header.Get("Authorization") != "" {
		return basicAuthor(w, req)
	} else {
		return normalAuthor(w, req)
	}
}

func basicAuthor(w http.ResponseWriter, req *http.Request) bool {
	UserName, Password, ok := req.BasicAuth()
	if !ok || UserName == "" {
		w.Write(returnDataResult(false, "Author error", ""))
		return false
	}
	pwd := config.GetConfigVal("user", UserName)
	if pwd == Password {
		GroupName := config.GetConfigVal("groups", UserName)
		if GroupName != "administrator" && checkWriteRequest(req.RequestURI) {
			w.Write(returnDataResult(false, "user group : [ "+GroupName+" ] no authority", ""))
			return false
		}
		return true
	} else {
		w.Write(returnDataResult(false, "Password error", ""))
	}
	return false
}

func normalAuthor(w http.ResponseWriter, req *http.Request) bool {
	var sessionID = sessionMgr.CheckCookieValid(w, req)

	if sessionID != "" {
		if _, ok := sessionMgr.GetSessionVal(sessionID, "UserName"); ok {
			//非administrator用户 用户，没有写操作权限
			Group, _ := sessionMgr.GetSessionVal(sessionID, "Group")
			if Group.(string) != "administrator" && checkWriteRequest(req.RequestURI) {
				w.Write(returnDataResult(false, "user group : [ "+Group.(string)+" ] no authority", ""))
				return false
			}
			return true
		} else {
			goto toLogin
		}
	} else {
		goto toLogin
	}

toLogin:
	if req.RequestURI != "/login" && req.RequestURI != "/dologin" && req.RequestURI != "/logout" {
		http.Redirect(w, req, "/login", http.StatusFound)
		return false
	}
	return true
}

func Start(IpAndPort string) {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
		}
	}()
	sessionMgr = xgo.NewSessionMgr("xgo_cookie", 3600)
	xgo.AddStaticRoute("/css/", TemplatePath("/manager/public/"))
	xgo.AddStaticRoute("/js/", TemplatePath("/manager/public/"))
	xgo.AddStaticRoute("/fonts/", TemplatePath("/manager/public/"))
	xgo.AddStaticRoute("/img/", TemplatePath("/manager/public/"))
	xgo.AddStaticRoute("/plugin/", TemplatePath("/"))
	xgo.SetFirstCallBack(controller_FirstCallback)
	var err error
	if config.TLS {
		err = xgo.StartTLS(IpAndPort, config.TLSServerKeyFile, config.TLSServerCrtFile)
	} else {
		err = xgo.Start(IpAndPort)
	}
	if err != nil {
		log.Println("Manager Start Err:", err)
	}
}
