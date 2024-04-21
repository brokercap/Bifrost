package server

import (
	"encoding/json"
	"fmt"
	"github.com/brokercap/Bifrost/config"
	"github.com/brokercap/Bifrost/server/storage"
	"hash/crc32"
	"strconv"
	"sync"
	"time"
)

type PositionStruct struct {
	BinlogFileNum  int
	BinlogPosition uint32
	GTID           string
	Timestamp      uint32
	EventID        uint64
}

type TmpPositioinStruct struct {
	sync.RWMutex
	Data map[string]*PositionStruct
}

var toSaveDbConfigChan chan int8
var TmpPositioin []*TmpPositioinStruct

var cachePoolCount uint32 = 0

func init() {
	toSaveDbConfigChan = make(chan int8, 100)
	go func() {
		timer := time.NewTimer(5 * time.Minute)
		for {
			select {
			case i := <-toSaveDbConfigChan:
				if i == 0 {
					return
				}
				break
			case <-timer.C:
				timer.Reset(5 * time.Minute)
				break
			}
			DoSaveSnapshotData()
		}
	}()
}

func InitStorage() {
	storage.InitStorage()
	cachePoolCount = uint32(config.KeyCachePoolSize)
	TmpPositioin = make([]*TmpPositioinStruct, cachePoolCount)
	if cachePoolCount > 0 {
		var i uint32 = 0
		for i = 0; i < cachePoolCount; i++ {
			TmpPositioin[i] = &TmpPositioinStruct{
				Data: make(map[string]*PositionStruct, 0),
			}
		}
		go saveBinlogPositionToStorageFromCache()
	}
}

func saveBinlogPositionToStorageFromCache() {
	for {
		time.Sleep(2 * time.Second)
		for _, t := range TmpPositioin {
			t.Lock()
			for k, v := range t.Data {
				Val, _ := json.Marshal(v)
				storage.PutKeyVal([]byte(k), Val)
			}
			t.Data = make(map[string]*PositionStruct, 0)
			t.Unlock()
		}
	}
}

var crc_table *crc32.Table = crc32.MakeTable(0xD5828281)

func saveBinlogPositionByCache(key []byte, t *PositionStruct) {
	if cachePoolCount <= 0 {
		saveBinlogPosition(key, t)
		return
	}
	id := crc32.Checksum(key, crc_table) % cachePoolCount
	TmpPositioin[id].Lock()
	TmpPositioin[id].Data[string(key)] = t
	TmpPositioin[id].Unlock()
}

func getBinlogPositionByCache(key []byte) (*PositionStruct, error) {
	id := crc32.Checksum(key, crc_table) % cachePoolCount
	TmpPositioin[id].RLock()
	defer TmpPositioin[id].RUnlock()
	if _, ok := TmpPositioin[id].Data[string(key)]; ok {
		return TmpPositioin[id].Data[string(key)], nil
	} else {
		return &PositionStruct{}, fmt.Errorf("no found")
	}
}

func SaveDBConfigInfo() {
	if toSaveDbConfigChan != nil {
		toSaveDbConfigChan <- 1
	}
}

func getToServerLastBinlogkey(db *db, toserver *ToServer) []byte {
	return []byte("last-binlog-toserver-" + db.Name + "-" + strconv.FormatInt(db.AddTime, 10) + "-" + toserver.ToServerKey + "-" + strconv.Itoa(toserver.ToServerID))
}

func getToServerBinlogkey(db *db, toserver *ToServer) []byte {
	return []byte("binlog-toserver-" + db.Name + "-" + strconv.FormatInt(db.AddTime, 10) + "-" + toserver.ToServerKey + "-" + strconv.Itoa(toserver.ToServerID))
}

func getDBBinlogkey(db *db) []byte {
	return []byte("binlog-db-" + db.Name + "-" + strconv.FormatInt(db.AddTime, 10))
}

func saveBinlogPosition(key []byte, t *PositionStruct) error {
	Val, _ := json.Marshal(t)
	err := storage.PutKeyVal(key, Val)
	return err
}

func getBinlogPosition(key []byte) (*PositionStruct, error) {
	if cachePoolCount > 0 {
		data0, err := getBinlogPositionByCache(key)
		if err == nil {
			return data0, nil
		}
	}
	s, err := storage.GetKeyVal(key)
	if err != nil {
		return nil, err
	}
	if len(s) == 0 {
		return nil, fmt.Errorf("not found data")
	}
	var data PositionStruct
	err2 := json.Unmarshal(s, &data)
	if err2 != nil {
		return nil, err2
	}
	return &data, nil
}

func delBinlogPosition(key []byte) error {
	return storage.DelKeyVal(key)
}

func Close() {
	storage.Close()
}
