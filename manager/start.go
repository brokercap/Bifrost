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
	"net/http"
	"encoding/json"
	"strconv"
	"github.com/Bifrost/manager/xgo"
	"log"
	"os/exec"
	"os"
	"path/filepath"
)

var execDir string

func init(){
	execPath, _ := exec.LookPath(os.Args[0])
	execDir = filepath.Dir(execPath)+"/"
}

func TemplatePath(fileName string) string{
	return execDir+fileName
}

type TemplateHeader struct {
	Title string
}

func (TemplateHeader *TemplateHeader) setTile(title string){
	TemplateHeader.Title = title
}

type resultStruct struct {
	Status bool `json:"status"`
	Msg string `json:"msg"`
}

var sessionMgr *xgo.SessionMgr = nil //session管理器

func returnResult(r bool,msg string)[]byte{
	b,_:=json.Marshal(resultStruct{Status:r,Msg:msg})
	return  b
}

func GetFormInt(req *http.Request,key string) int{
	v := req.Form.Get(key)
	intv,err:=strconv.Atoi(v)
	if err != nil{
		return 0
	}else{
		return intv
	}
}

func AddRoute(route string, callbackFUns func(http.ResponseWriter,*http.Request) ){
	xgo.AddRoute(route,callbackFUns)
}

func index_controller(w http.ResponseWriter,req *http.Request){
	http.Redirect(w, req, "/db/list", http.StatusFound)
}

func Controller_FirstCallback(w http.ResponseWriter,req *http.Request) bool {
	var sessionID= sessionMgr.CheckCookieValid(w, req)

	if sessionID != "" {
		if _,ok:=sessionMgr.GetSessionVal(sessionID,"UserName");ok{
			return true
		}else{
			goto toLogin
		}
	}else{
		goto toLogin
	}

	toLogin:
		if req.RequestURI != "/login" && req.RequestURI != "/dologin" && req.RequestURI != "/logout"{
			http.Redirect(w, req, "/login", http.StatusFound)
			return false
		}
		return true
}

func Start(IpAndPort string){
	defer func() {
		if err:=recover(); err!= nil{
			log.Println(err)
		}
	}()
	sessionMgr = xgo.NewSessionMgr("xgo_cookie", 3600)
	xgo.AddStaticRoute("/css/",TemplatePath("manager/public/"))
	xgo.AddStaticRoute("/js/",TemplatePath("manager/public/"))
	xgo.AddStaticRoute("/fonts/",TemplatePath("manager/public/"))
	xgo.AddStaticRoute("/img/",TemplatePath("manager/public/"))
	xgo.SetFirstCallBack(Controller_FirstCallback)
	xgo.AddRoute("/",index_controller)
	xgo.Start(IpAndPort)
}


