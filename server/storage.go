package server

import (
	"github.com/jc3wish/Bifrost/server/storage"
	"encoding/json"
	"strconv"
	"fmt"
)

type positionStruct struct {
	BinlogFileNum int
	BinlogPosition uint32
}


var toSaveDbConfigChan chan int8

func init() {}

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
