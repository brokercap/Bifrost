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
	go func() {
		// 不要问我这里为什么要异步 ，并定时5秒，
		// 因为如果不这样的话，在删除了leveldb存储目录的情况下，再启动 GetListByPrefix 的时候，是没有数据的，这样就会 Put数据进去，但是leveldb 这里过一会会把老数据加载进来，覆盖这些数据
		time.Sleep( time.Duration(5) * time.Second)
		for Name,Password := range config.GetConf("user"){
			UserGroup := getUserGroup(config.GetConfigVal("groups",Name))
			User := UserInfo{
				Name:Name,
				Password:Password,
				Group:UserGroup,
				AddTime:time.Now().Unix(),
				UpdateTime:time.Now().Unix(),
			}
			b,_:=json.Marshal(User)
			err := storage.PutKeyVal([]byte(USER_PREFIX+Name),b)
			if err != nil{
				log.Println("InitUser error:",err," user:",User)
			}
		}
	}()
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
	userListString:= storage.GetListByPrefix([]byte(USER_PREFIX))
	if len(userListString) == 0 {
		return []UserInfo{}
	}
	UserList := make([]UserInfo,0)
	for _,v := range userListString{
		var User UserInfo
		err := json.Unmarshal([]byte(v.Value),&User)
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
		User.AddTime = OldUserInfo.AddTime
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