package user

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/brokercap/Bifrost/config"
	"github.com/brokercap/Bifrost/server/storage"
	"log"
	"strings"
	"time"
)

const USER_PREFIX string = "bifrost_UserList_"

type UserGroupType string

type UserInfo struct {
	Name       string
	Password   string
	Group      string
	Host       string
	AddTime    int64
	UpdateTime int64
}

func init() {

}

func getUserGroup(groupName string) string {
	if groupName != "administrator" {
		return "monitor"
	}
	return groupName
}

func InitUser() {
	userList := storage.GetListByPrefix([]byte(USER_PREFIX))
	//假如 userList 为空的情况下,则需要将 etc 配置文件中的用户名和密码导入到存储中
	if len(userList) != 0 {
		return
	}
	func() {
		// 假如是go 1.11的话 这里需要异步并且并定时5秒
		// 因为如果不这样的话，在删除了leveldb存储目录的情况下，再启动 GetListByPrefix 的时候，是没有数据的，这样就会 Put数据进去，但是leveldb 这里过一会会把老数据加载进来，覆盖这些数据
		// 可能是因为 leveldb 包里用了go 里的某个特性,go 1.11 中还存在bug
		// 所以这里我们并不异步,要求 go1.12+ 版本编译
		// time.Sleep( time.Duration(5) * time.Second)
		for Name, Password := range config.GetConf("user") {
			UserGroup := getUserGroup(config.GetConfigVal("groups", Name))
			User := UserInfo{
				Name:       Name,
				Password:   Password,
				Group:      UserGroup,
				Host:       "%",
				AddTime:    time.Now().Unix(),
				UpdateTime: time.Now().Unix(),
			}
			b, _ := json.Marshal(User)
			err := storage.PutKeyVal([]byte(USER_PREFIX+Name), b)
			if err != nil {
				log.Println("InitUser error:", err, " user:", User)
			}
		}
	}()
}

func RecoveryUser(content *json.RawMessage) {
	if content == nil {
		return
	}
	var data []*UserInfo
	errors := json.Unmarshal(*content, &data)
	if errors != nil {
		log.Println("recorery user content errors;", errors, " content:", content)
		return
	}

	for _, User := range data {
		b, _ := json.Marshal(User)
		storage.PutKeyVal([]byte(USER_PREFIX+User.Name), b)
	}
}

func GetUserList() []UserInfo {
	userListString := storage.GetListByPrefix([]byte(USER_PREFIX))
	if len(userListString) == 0 {
		return []UserInfo{}
	}
	UserList := make([]UserInfo, 0)
	for _, v := range userListString {
		var User UserInfo
		err := json.Unmarshal([]byte(v.Value), &User)
		if err == nil {
			UserList = append(UserList, User)
			if User.Name == "" {
				storage.DelKeyVal([]byte(v.Key))
			}
		}
	}
	return UserList
}

func DelUser(Name string) error {
	key := USER_PREFIX + Name
	return storage.DelKeyVal([]byte(key))
}

func AddUser(Name, Password, GroupName string, Host string) error {
	return UpdateUser(Name, Password, GroupName, Host)
}

func UpdateUser(Name, Password, GroupName string, Host string) error {
	if Name == "" || Password == "" {
		return fmt.Errorf("Name and Password not be empty!")
	}
	OldUserInfo := GetUserInfo(Name)
	User := &UserInfo{
		Name:     Name,
		Password: Password,
		Host:     Host,
		Group:    getUserGroup(GroupName),
	}
	if OldUserInfo.Name == "" {
		User.AddTime = time.Now().Unix()
		User.UpdateTime = time.Now().Unix()
	} else {
		User.AddTime = OldUserInfo.AddTime
		User.UpdateTime = time.Now().Unix()
	}
	key := USER_PREFIX + Name
	b, _ := json.Marshal(User)
	return storage.PutKeyVal([]byte(key), b)
}

func GetUserInfo(Name string) *UserInfo {
	b, err := storage.GetKeyVal([]byte(USER_PREFIX + Name))
	if err != nil {
		return &UserInfo{}
	}
	var User UserInfo
	err = json.Unmarshal(b, &User)
	if err != nil {
		return &UserInfo{}
	}
	return &User
}

func CheckUser(Name, Password string) (userInfo *UserInfo, err error) {
	userInfo = GetUserInfo(Name)
	if userInfo.Name == "" {
		err = errors.New("user not exsit!")
		return
	}
	if userInfo.Password != Password {
		err = errors.New("password error!")
		return
	}
	return
}

func CheckUserWithIP(Name, Password string, IP string) (userInfo *UserInfo, err error) {
	if IP != "127.0.0.1" && CheckRefuseIp(IP) {
		return nil, errors.New("ip is refused")
	}
	userInfo, err = CheckUser(Name, Password)
	if err != nil {
		AddFailedIp(IP)
		appendLoginLog("IP:%s UserName:%s Password:%s login failed", IP, Name, Password)
		return
	}
	err = CheckUserHost(IP, userInfo.Host)
	if userInfo.Group == "" {
		userInfo.Group = "monitor"
	}
	if err != nil {
		AddFailedIp(IP)
		appendLoginLog("IP:%s UserName:%s CheckUserHost failed", IP, Name)
	} else {
		appendLoginLog("IP:%s UserName:%s login success", IP, Name)
	}
	return
}

func CheckUserHost(IP, Host string) (err error) {
	if IP == Host {
		return
	}
	switch Host {
	case "", "%":
		return
	default:
		break
	}
	ipArr := strings.Split(IP, ".")
	var ok bool
	for _, HostName := range strings.Split(Host, ",") {
		ok = CheckUserHost0(ipArr, HostName)
		if ok {
			return nil
		}
	}
	return errors.New("No login permission!")
}

func CheckUserHost0(ipArr []string, Host string) bool {
	switch Host {
	case "%":
		return true
	default:
		break
	}
	hostArr := strings.Split(Host, ".")
	for i, v := range hostArr {
		if v == "%" {
			continue
		}
		if ipArr[i] == v {
			continue
		}
		return false
	}
	return true
}
