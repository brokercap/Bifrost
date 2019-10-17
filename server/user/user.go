package user

import (
	"github.com/brokercap/Bifrost/config"
	"github.com/brokercap/Bifrost/server/storage"
	"encoding/json"
	"log"
	"fmt"
	"time"
)

const USER_PREFIX  string = "Birost_UserList_"

type UserGroupType string

const (
	Administrator UserGroupType = "administrator"
	Monitor UserGroupType = "monitor"
)

type UserInfo struct {
	Name string
	Password string
	Group string
	AddTime int64
	UpdateTime int64
}

func init()  {

}

func getUserGroup(groupName string) string  {
	if groupName != "administrator"{
		return "monitor"
	}
	return groupName
}

func InitUser()  {
	userList := storage.GetListByPrefix([]byte(USER_PREFIX))
	//假如 userList 为空的情况下,则需要将 etc 配置文件中的用户名和密码导入到存储中
	if len(userList) != 0{
		return
	}

	for Name,Password := range config.GetConf("user"){
		UserGroup := getUserGroup(config.GetConfigVal("groups",Name))
		User := UserInfo{
			Name:Name,
			Password:Password,
			Group:UserGroup,
		}
		b,_:=json.Marshal(User)
		storage.PutKeyVal([]byte(USER_PREFIX+Name),b)
	}
}


func RecoveryUser(content *json.RawMessage)  {
	InitUser()
	if content == nil{
		return
	}
	var data []*UserInfo
	errors := json.Unmarshal(*content,&data)
	if errors != nil{
		log.Println( "recorery user content errors;",errors," content:",content)
		return
	}

	for _,User := range data{
		b,_:=json.Marshal(User)
		storage.PutKeyVal([]byte(USER_PREFIX+User.Name),b)
	}
}


func GetUserList() []UserInfo {
	userListBytes:= storage.GetListByPrefix([]byte(USER_PREFIX))
	if len(userListBytes) == 0 {
		return []UserInfo{}
	}
	UserList := make([]UserInfo,0)
	for _,v := range userListBytes{
		var User UserInfo
		dd := v[1]
		err := json.Unmarshal(dd,&User)
		if err == nil{
			UserList = append(UserList,User)
		}
	}
	return UserList
}

func DelUser(Name string) error {
	key := USER_PREFIX+Name
	return storage.DelKeyVal([]byte(key))
}

func AddUser(Name,Password,GroupName string ) error {
	return UpdateUser(Name,Password,GroupName)
}

func UpdateUser(Name,Password,GroupName string ) error {
	if Name == "" || Password == ""{
		return fmt.Errorf("Name and Password not be empty!")
	}
	OldUserInfo := GetUserInfo(Name)

	User := &UserInfo{
		Name:Name,
		Password:Password,
		Group:getUserGroup(GroupName),
	}
	if OldUserInfo.Name == ""{
		User.AddTime = time.Now().Unix()
		User.UpdateTime = time.Now().Unix()
	}else{
		User.UpdateTime = time.Now().Unix()
	}
	key := USER_PREFIX+Name
	b,_:=json.Marshal(User)
	return storage.PutKeyVal([]byte(key),b)
}

func GetUserInfo(Name string) UserInfo {
	b,err := storage.GetKeyVal([]byte(USER_PREFIX+Name))
	if err != nil{
		return UserInfo{}
	}
	var User UserInfo
	err = json.Unmarshal(b,&User)
	if err != nil{
		return UserInfo{}
	}
	return User
}