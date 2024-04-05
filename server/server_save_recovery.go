package server

import (
	"encoding/json"
	"github.com/brokercap/Bifrost/config"
	"github.com/brokercap/Bifrost/plugin"
	"github.com/brokercap/Bifrost/server/storage"
	"github.com/brokercap/Bifrost/server/user"
	"github.com/brokercap/Bifrost/server/warning"
	"log"
	"sync"
	"time"
)

var l sync.RWMutex

type recovery struct {
	Version   string
	StartTime time.Time
	ToServer  *json.RawMessage
	DbInfo    *json.RawMessage
	User      *json.RawMessage
	Warning   *json.RawMessage
}

type recoveryDataSturct struct {
	Version   string
	StartTime time.Time
	ToServer  interface{}
	DbInfo    interface{}
	User      interface{}
	Warning   interface{}
}

func DoRecoverySnapshotData() {

	//这里初始化用户,第一次启动的情况下,配置文件中的用户需要初始化
	user.InitUser()

	fd, err := storage.GetDBInfo()
	if err != nil {
		return
	}
	if string(fd) == "" {
		return
	}
	var data recovery
	errors := json.Unmarshal(fd, &data)
	if errors != nil {
		log.Printf("recovery error:%s, data:%s \r\n", errors, string(fd))
		return
	}
	setServerStartTime(data.StartTime)
	if data.ToServer != nil && string(*data.ToServer) != "{}" {
		plugin.Recovery(data.ToServer)
	}
	if data.DbInfo != nil && string(*data.DbInfo) != "{}" {
		Recovery(data.DbInfo, false)
	}
	if data.User != nil && string(*data.User) != "[]" {
		user.RecoveryUser(data.User)
	}

	if data.Warning != nil && string(*data.Warning) != "{}" {
		warning.RecoveryWarning(data.Warning)
	}

}

func GetSnapshotData() ([]byte, error) {
	l.Lock()
	defer func() {
		l.Unlock()
		if err := recover(); err != nil {
			log.Println(err)
		}
	}()
	data := recoveryDataSturct{
		Version:   config.VERSION,
		StartTime: GetServerStartTime(),
		ToServer:  plugin.SaveToServerData(),
		DbInfo:    SaveDBInfoToFileData(),
		User:      user.GetUserList(),
		Warning:   warning.GetWarningConfigList(),
	}
	return json.Marshal(data)
}

// 只获取 数据源 和 目标库的镜像数据
func GetSnapshotData2() ([]byte, error) {
	l.Lock()
	defer func() {
		l.Unlock()
		if err := recover(); err != nil {
			log.Println(err)
		}
	}()
	data := recoveryDataSturct{
		Version:   config.VERSION,
		StartTime: GetServerStartTime(),
		ToServer:  plugin.SaveToServerData(),
		DbInfo:    SaveDBInfoToFileData(),
	}
	return json.Marshal(data)
}

func DoSaveSnapshotData() {
	var data []byte
	var err error
	for i := 0; i < 3; i++ {
		data, err = GetSnapshotData2()
		if err == nil {
			break
		}
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
	if err != nil {
		SaveDBConfigInfo()
		return
	}
	storage.SaveDBInfo(data)
}

func DoRecoveryByBackupData(fileContent string) {
	var data recovery
	errors := json.Unmarshal([]byte(fileContent), &data)
	if errors != nil {
		log.Printf("recovery error:%s, data:%s \r\n", errors, fileContent)
		return
	}
	setServerStartTime(data.StartTime)
	if string(*data.ToServer) != "{}" {
		plugin.Recovery(data.ToServer)
	}
	if string(*data.DbInfo) != "{}" {
		Recovery(data.DbInfo, true)
	}
	if string(*data.Warning) != "{}" {
		warning.RecoveryWarning(data.Warning)
	}
	if string(*data.User) != "[]" {
		user.RecoveryUser(data.User)
	}
}
