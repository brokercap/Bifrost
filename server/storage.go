package server

import (
	"github.com/jc3wish/Bifrost/server/storage"
	"encoding/json"
	"strconv"
	"fmt"
	"sync"
	"hash/crc32"
	"time"
)

type positionStruct struct {
	BinlogFileNum int
	BinlogPosition uint32
}

type TmpPositioinStruct struct {
	sync.Mutex
	Data map[string]positionStruct
}

var toSaveDbConfigChan chan int8
var TmpPositioin []TmpPositioinStruct

func init() {
	TmpPositioin = make([]TmpPositioinStruct,100)
	for i:=0;i<100;i++{
		TmpPositioin[i] = TmpPositioinStruct{
			Data:make(map[string]positionStruct,0),
		}
	}
	go saveBinlogPositionToStorageFromCache()
}

func saveBinlogPositionToStorageFromCache()  {
	for {
		time.Sleep(2 * time.Second)
		for _, t := range TmpPositioin {
			t.Lock()
			for k, v := range t.Data {
				Val, _ := json.Marshal(v)
				storage.PutKeyVal([]byte(k) , Val)
				delete(t.Data,k)
			}
			t.Unlock()
		}
	}
}

var crc_table *crc32.Table = crc32.MakeTable(0xD5828281)

func saveBinlogPositionByCache(key []byte,BinlogFileNum int,BinlogPosition uint32)  {
	id := crc32.Checksum(key, crc_table) % 100
	TmpPositioin[id].Lock()
	TmpPositioin[id].Data[string(key)]=positionStruct{BinlogFileNum,BinlogPosition}
	TmpPositioin[id].Unlock()
}

func InitStrageChan(ch chan int8){
	toSaveDbConfigChan = ch
}

func SaveDBConfigInfo(){
	if toSaveDbConfigChan != nil{
		toSaveDbConfigChan <- 1
	}
}

func InitStorage(){
	storage.InitStorage()
}

func getToServerLastBinlogkey(db *db ,toserver *ToServer) []byte{
	return []byte("last-binlog-toserver-"+db.Name+"-"+strconv.FormatInt(db.AddTime, 10)+"-"+toserver.ToServerKey+"-"+strconv.Itoa(toserver.ToServerID))
}

func getToServerBinlogkey(db *db ,toserver *ToServer) []byte{
	return []byte("binlog-toserver-"+db.Name+"-"+strconv.FormatInt(db.AddTime, 10)+"-"+toserver.ToServerKey+"-"+strconv.Itoa(toserver.ToServerID))
}

func getDBBinlogkey(db *db) []byte{
	return []byte("binlog-db-"+db.Name+"-"+strconv.FormatInt(db.AddTime, 10))
}

func saveBinlogPosition(key []byte,BinlogFileNum int,BinlogPosition uint32) error {
	f := positionStruct{BinlogFileNum,BinlogPosition}
	Val,_ := json.Marshal(f)
	err := storage.PutKeyVal(key, Val)
	return err
}

func getBinlogPosition(key []byte) (*positionStruct,error) {
	s, err := storage.GetKeyVal(key)
	if err != nil{
		return nil,err
	}
	if len(s) == 0{
		return nil,fmt.Errorf("not found data")
	}
	var data positionStruct
	err2 := json.Unmarshal(s,&data)
	if err2 != nil{
		return nil,err2
	}
	return &data,nil
}

func delBinlogPosition(key []byte) error {
	return storage.DelKeyVal(key)
}
