package server

import (
	"fmt"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	pluginStorage "github.com/brokercap/Bifrost/plugin/storage"
	"github.com/brokercap/Bifrost/server/filequeue"
	"log"
	"sync"
)

type ToServerStatus string

type ToServer struct {
	sync.RWMutex
	Key           *string `json:"-"` // 上一级的key
	ToServerID    int
	PluginName    string
	MustBeSuccess bool
	FilterQuery   bool
	FilterUpdate  bool
	FieldList     []string
	ToServerKey   string

	LastSuccessBinlog *PositionStruct // 最后处理成功的位点信息
	LastQueueBinlog   *PositionStruct // 最后进入队列的位点信息

	BinlogFileNum  int    // 支持到 1.8.x
	BinlogPosition uint32 // 支持到 1.8.x

	PluginParam   map[string]interface{}
	Status        StatusFlag
	ToServerChan  *ToServerChan `json:"-"`
	Error         string
	ErrorWaitDeal int
	ErrorWaitData *pluginDriver.PluginDataType

	LastBinlogFileNum  int    // 由 channel 提交到 ToServerChan 的最后一个位点 // 将会在 1.8.x 版本开始去掉这个字段
	LastBinlogPosition uint32 // 假如 BinlogFileNum == LastBinlogFileNum && BinlogPosition == LastBinlogPosition 则说明这个位点是没有问题的  // 支持到 1.8.x

	LastBinlogKey                 []byte `json:"-"` // 将数据保存到 level 的key
	QueueMsgCount                 uint32 // 队列里的堆积的数量
	fileQueueObj                  *filequeue.Queue
	FileQueueStatus               bool // 是否启动文件队列
	Notes                         string
	ThreadCount                   int16  // 消费线程数量
	FileQueueUsableCount          uint32 // 在开始文件队列的配置下，每次写入 ToServerChan 后 ，在 FileQueueUsableCountTimeDiff 时间内 队列都是满的次数
	FileQueueUsableCountStartTime int64  // 开始统计 FileQueueUsableCount 计算的时间
	statusChan                    chan bool
	cosumerPluginParamArr         []interface{} `json:"-"` // 用以区分多个消费者的身份
}

/*
*
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

	var Binlog0 = &PositionStruct{}
	if toserver.LastQueueBinlog == nil {
		BinlogPostion, err := getBinlogPosition(getDBBinlogkey(db))
		if err == nil {
			// 这里手工复制的原因是要防止 数据出错
			Binlog0 = &PositionStruct{
				BinlogFileNum:  BinlogPostion.BinlogFileNum,
				BinlogPosition: BinlogPostion.BinlogPosition,
				GTID:           BinlogPostion.GTID,
				Timestamp:      BinlogPostion.Timestamp,
				EventID:        BinlogPostion.EventID,
			}
		}
		toserver.LastQueueBinlog = Binlog0
		toserver.LastSuccessBinlog = Binlog0
	}
	if toserver.LastSuccessBinlog == nil {
		toserver.LastSuccessBinlog = Binlog0
	}

	toserver.Key = &key
	toserver.QueueMsgCount = 0
	toserver.statusChan = make(chan bool, 1)
	db.tableMap[key].ToServerList = append(db.tableMap[key].ToServerList, toserver)

	// 在添加第一个同步的时候，通知 binlog 解析，需要同步这个表
	if len(db.tableMap[key].ToServerList) == 1 && db.inputDriverObj != nil {
		db.AddReplicateDoDb(schemaName, tableName, false)
	}
	log.Println("AddTableToServer", db.Name, schemaName, tableName, toserver)
	return true, toserver.ToServerID
}

/*
*
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

	if toServerInfo.Status == RUNNING || toServerInfo.Status == STOPPING {
		toServerInfo.Status = DELING
	} else {
		if toServerInfo.Status != DELING {
			delBinlogPosition(toServerPositionBinlogKey)
		}
	}
	// 当前这个表都没有同步配置了，则通知 binlog 解析，不再需要解析这个表的数据了
	if len(db.tableMap[key].ToServerList) == 0 && db.inputDriverObj != nil {
		db.DelReplicateDoDb(schemaName, tableName, false)
	}
	log.Println("DelTableToServer", db.Name, schemaName, tableName, "toServerInfo:", toServerInfo)

	//将文件队列的路径也相应的删除掉
	filequeue.Delete(GetFileQueue(db.Name, schemaName, tableName, fmt.Sprint(ToServerID)))
	return true
}

func (This *ToServer) UpdateBinlogPosition(BinlogFileNum int, BinlogPosition uint32, GTID string, Timestamp uint32) bool {
	This.Lock()
	This.LastSuccessBinlog = &PositionStruct{
		BinlogFileNum:  BinlogFileNum,
		BinlogPosition: BinlogPosition,
		GTID:           GTID,
		Timestamp:      Timestamp,
		EventID:        0,
	}
	This.Unlock()
	return true
}

func (This *ToServer) AddWaitError(WaitErr error, WaitData *pluginDriver.PluginDataType) bool {
	This.Lock()
	This.Error = WaitErr.Error()
	This.ErrorWaitData = WaitData
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
