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
	"html/template"
	"net/http"

	"encoding/json"
	"github.com/brokercap/Bifrost/server/user"
)

func init() {
	addRoute("/login", user_login)
	addRoute("/dologin", user_do_login)
	addRoute("/logout", user_logout)

	addRoute("/user/update", update_user_controller)
	addRoute("/user/del", del_user_controller)
	addRoute("/user/list", list_user_controller)
}

func user_login(w http.ResponseWriter, req *http.Request) {
	data := TemplateHeader{Title: "Login - Bifrost"}
	t, _ := template.ParseFiles(TemplatePath("/manager/template/login.html"))
	t.Execute(w, data)
}

func user_logout(w http.ResponseWriter, req *http.Request) {
	sessionMgr.EndSession(w, req) //用户退出时删除对应session
	http.Redirect(w, req, "/login", http.StatusFound)
}

func user_do_login(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	var sessionID = sessionMgr.StartSession(w, req)
	UserName := req.Form.Get("user_name")
	UserPwd := req.Form.Get("password")

	if UserName == "" {
		w.Write(returnResult(false, " user no exsit"))
		return
	}

	UserInfo := user.GetUserInfo(UserName)
	if UserInfo.Password == UserPwd {
		GroupName := UserInfo.Group
		if GroupName == "" {
			GroupName = "monitor"
		}
		sessionMgr.SetSessionVal(sessionID, "UserName", UserName)
		sessionMgr.SetSessionVal(sessionID, "Group", GroupName)
		w.Write(returnResult(true, " success"))
		return
	}
	w.Write(returnResult(false, " user error"))
	return
}

func user_update(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	UserName := req.Form.Get("user_name")
	UserPwd := req.Form.Get("password")
	UserGroup := req.Form.Get("group")
	if UserName == "" || UserPwd == "" {
		w.Write(returnResult(false, " user_name and password not empty!"))
		return
	}
	err := user.UpdateUser(UserName, UserPwd, UserGroup)
	if err != nil {
		w.Write(returnResult(false, err.Error()))
	} else {
		w.Write(returnResult(true, "success"))
	}
	return
}

func update_user_controller(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	UserName := req.Form.Get("user_name")
	UserPwd := req.Form.Get("password")
	UserGroup := req.Form.Get("group")
	if UserName == "" || UserPwd == "" {
		w.Write(returnResult(false, " user_name and password not empty!"))
		return
	}
	err := user.UpdateUser(UserName, UserPwd, UserGroup)
	if err != nil {
		w.Write(returnResult(false, err.Error()))
	} else {
		w.Write(returnResult(true, "success"))
	}
	return
}

func del_user_controller(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	UserName := req.Form.Get("user_name")
	if UserName == "" {
		w.Write(returnResult(false, " user_name not empty!"))
		return
	}
	err := user.DelUser(UserName)
	if err != nil {
		w.Write(returnResult(false, err.Error()))
	} else {
		w.Write(returnResult(true, "success"))
	}
	return
}

func list_user_controller(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	UserList := user.GetUserList()

	//过滤密码,防止其他 monitor 用户查看到
	for k, _ := range UserList {
		UserList[k].Password = ""
	}

	if req.Form.Get("format") == "json" {
		data, _ := json.Marshal(UserList)
		w.Write(data)
		return
	}

	type UserListStruct struct {
		TemplateHeader
		UserList []user.UserInfo
	}

	UserListInfo := UserListStruct{UserList: UserList}
	UserListInfo.Title = "UserList-Bifrost"
	t, _ := template.ParseFiles(TemplatePath("/manager/template/user.list.html"), TemplatePath("/manager/template/header.html"), TemplatePath("/manager/template/footer.html"))
	t.Execute(w, UserListInfo)
	return
}
