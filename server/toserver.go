package server

import (
	"fmt"
	pluginStorage "github.com/brokercap/Bifrost/plugin/storage"
	"github.com/brokercap/Bifrost/server/filequeue"
	"log"
	"sync"

	"encoding/json"
)

type ToServerStatus string

type ToServer struct {
	sync.RWMutex
	Key                           *string `json:"-"` // 上一级的key
	ToServerID                    int
	PluginName                    string
	MustBeSuccess                 bool
	FilterQuery                   bool
	FilterUpdate                  bool
	FieldList                     []string
	ToServerKey                   string
	BinlogFileNum                 int
	BinlogPosition                uint32
	PluginParam                   map[string]interface{}
	Status                        string
	ToServerChan                  *ToServerChan `json:"-"`
	Error                         string
	ErrorWaitDeal                 int
	ErrorWaitData                 interface{}
	LastBinlogFileNum             int    // 由 channel 提交到 ToServerChan 的最后一个位点
	LastBinlogPosition            uint32 // 假如 BinlogFileNum == LastBinlogFileNum && BinlogPosition == LastBinlogPosition 则说明这个位点是没有问题的
	LastBinlogKey                 []byte `json:"-"` // 将数据保存到 level 的key
	QueueMsgCount                 uint32 // 队列里的堆积的数量
	fileQueueObj                  *filequeue.Queue
	FileQueueStatus               bool // 是否启动文件队列
	Notes                         string
	ThreadCount                   int16                  // 消费线程数量
	FileQueueUsableCount          uint32                 // 在开始文件队列的配置下，每次写入 ToServerChan 后 ，在 FileQueueUsableCountTimeDiff 时间内 队列都是满的次数
	FileQueueUsableCountStartTime int64                  // 开始统计 FileQueueUsableCount 计算的时间
	CosumerPluginParamMap         map[uint16]interface{} `json:"-"` // 用以区分多个消费者的身份
	CosumerIdInrc                 uint16                 // 消费者自增id
	statusChan                    chan bool
}
/**
新增表的同步配置
假如是第一次添加的表同步配置，则需要通知 binlog 解析库，解析当前表的binlog
*/
func (db *db) AddTableToServer(schemaName string, tableName string, toserver *ToServer) (bool, int) {
	key := GetSchemaAndTableJoin(schemaName, tableName)
	db.Lock()
	defer db.Unlock()
	if _, ok := db.tableMap[key]; !ok {
		return false, 0
	}
	if toserver.ToServerID <= 0 {
		db.tableMap[key].LastToServerID += 1
		toserver.ToServerID = db.tableMap[key].LastToServerID
	}
	if toserver.PluginName == "" {
		ToServerInfo := pluginStorage.GetToServerInfo(toserver.ToServerKey)
		if ToServerInfo != nil {
			toserver.PluginName = ToServerInfo.PluginName
		}
	}
	if toserver.BinlogFileNum == 0 {
		BinlogPostion, err := getBinlogPosition(getDBBinlogkey(db))
		if err == nil {
			toserver.BinlogFileNum = BinlogPostion.BinlogFileNum
			toserver.LastBinlogFileNum = BinlogPostion.BinlogFileNum
			toserver.BinlogPosition = BinlogPostion.BinlogPosition
			toserver.LastBinlogPosition = BinlogPostion.BinlogPosition
		} else {
			log.Println("AddTableToServer GetDBBinlogPostion:", err)
		}
	}
	toserver.Key = &key
	toserver.QueueMsgCount = 0
	toserver.statusChan = make(chan bool,1)
	db.tableMap[key].ToServerList = append(db.tableMap[key].ToServerList, toserver)

	// 在添加第一个同步的时候，通知 binlog 解析，需要同步这个表
	if len(db.tableMap[key].ToServerList) == 1 && db.binlogDump != nil {
		db.AddReplicateDoDb(schemaName, tableName,false)
	}
	log.Println("AddTableToServer", db.Name, schemaName, tableName, toserver)
	return true, toserver.ToServerID
}

/**
删除表的同步配置
假如当前表没有其他同步配置了，则需要从 binlog 解析中删除掉，不再需要 解析这个表的数据
*/
func (db *db) DelTableToServer(schemaName string, tableName string, ToServerID int) bool {
	key := GetSchemaAndTableJoin(schemaName, tableName)
	db.Lock()
	defer db.Unlock()
	if _, ok := db.tableMap[key]; !ok {
		return false
	}
	var index int = -1
	for index1, toServerInfo2 := range db.tableMap[key].ToServerList {
		if toServerInfo2.ToServerID == ToServerID {
			index = index1
			break
		}
	}
	if index == -1 {
		return true
	}
	toServerInfo := db.tableMap[key].ToServerList[index]
	toServerPositionBinlogKey := getToServerBinlogkey(db, toServerInfo)
	if index == len(db.tableMap[key].ToServerList)-1 {
		db.tableMap[key].ToServerList = db.tableMap[key].ToServerList[:len(db.tableMap[key].ToServerList)-1]
	} else {
		db.tableMap[key].ToServerList = append(db.tableMap[key].ToServerList[:index], db.tableMap[key].ToServerList[index+1:]...)
	}

	if toServerInfo.Status == "running" || toServerInfo.Status == "stopping" {
		toServerInfo.Status = "deling"
	} else {
		if toServerInfo.Status != "deling" {
			delBinlogPosition(toServerPositionBinlogKey)
		}
	}
	// 当前这个表都没有同步配置了，则通知 binlog 解析，不再需要解析这个表的数据了
	if len(db.tableMap[key].ToServerList) == 0 && db.binlogDump != nil {
		db.DelReplicateDoDb(schemaName, tableName,false)
	}
	log.Println("DelTableToServer", db.Name, schemaName, tableName, "toServerInfo:", toServerInfo)

	//将文件队列的路径也相应的删除掉
	filequeue.Delete(GetFileQueue(db.Name, schemaName, tableName, fmt.Sprint(ToServerID)))
	return true
}

func (This *ToServer) UpdateBinlogPosition(BinlogFileNum int, BinlogPosition uint32) bool {
	This.Lock()
	This.BinlogFileNum = BinlogFileNum
	This.BinlogPosition = BinlogPosition
	This.Unlock()
	return true
}

func (This *ToServer) AddWaitError(WaitErr error, WaitData interface{}) bool {
	This.Lock()
	This.Error = WaitErr.Error()
	b, _ := json.Marshal(WaitData)
	This.ErrorWaitData = string(b)
	This.Unlock()
	return true
}

func (This *ToServer) DealWaitError() bool {
	This.Lock()
	This.ErrorWaitDeal = 1
	This.Unlock()
	return true
}

func (This *ToServer) GetWaitErrorDeal() int {
	This.Lock()
	deal := This.ErrorWaitDeal
	This.Unlock()
	return deal
}

func (This *ToServer) DelWaitError() bool {
	This.Lock()
	This.Error = ""
	This.ErrorWaitData = nil
	This.ErrorWaitDeal = 0
	This.Unlock()
	return true
}
