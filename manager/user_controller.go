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
	"html/template"
	"github.com/jc3wish/Bifrost/config"

)

func init()  {
	addRoute("/login",user_login)
	addRoute("/dologin",user_do_login)
	addRoute("/logout",user_logout)
}

func user_login(w http.ResponseWriter,req *http.Request){
	data := TemplateHeader{Title:"Login - Bifrost"}
	t, _ := template.ParseFiles(TemplatePath("manager/template/login.html"))
	t.Execute(w, data)
}

func user_do_login(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	var sessionID = sessionMgr.StartSession(w, req)
	UserName := req.Form.Get("user_name")
	UserPwd := req.Form.Get("password")

	if UserName == ""{
		w.Write(returnResult(false," user no exsit"))
		return
	}
	pwd := config.GetConfigVal("user",UserName)
	if pwd == UserPwd{
		sessionMgr.SetSessionVal(sessionID, "UserName", UserName)
		w.Write(returnResult(true," success"))
		return
	}
	w.Write(returnResult(false," user error"))
	return
}

func user_logout(w http.ResponseWriter,req *http.Request){
	sessionMgr.EndSession(w, req) //用户退出时删除对应session
	http.Redirect(w, req, "/login", http.StatusFound)
}