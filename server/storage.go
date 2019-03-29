package server

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/jc3wish/Bifrost/config"
	"path/filepath"
	"os"
	"log"
	"encoding/json"
	"strconv"
	"fmt"
)

type positionStruct struct {
	BinlogFileNum int
	BinlogPosition uint32
}

var levelDB *leveldb.DB

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
	var err error
	dataDir := config.GetConfigVal("Bifrostd","data_dir")
	if dataDir == ""{
		execDir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
		dataDir = execDir+"/data"
	}
	levelDbPath := dataDir+"/leveldb"
	os.MkdirAll(levelDbPath, 0700)
	levelDB, err = leveldb.OpenFile(levelDbPath, nil)
	if err != nil{
		log.Println("init leveldb err:",err)
		os.Exit(0)
	}
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
	err := levelDB.Put(key, Val, nil)
	return err
}

func getBinlogPosition(key []byte) (*positionStruct,error) {
	s, err := levelDB.Get(key, nil)
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
	return levelDB.Delete(key,nil)
}
