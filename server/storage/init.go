package storage

/*
import (
"github.com/syndtr/goleveldb/leveldb"
"github.com/syndtr/goleveldb/leveldb/util"
"github.com/brokercap/Bifrost/config"
"path/filepath"
"os"
"log"
)

var levelDB *leveldb.DB

var toSaveDbConfigChan chan int8

func init() {}

func InitStorage(){
	var err error
	dataDir := config.GetConfigVal("Bifrostd","data_dir")
	if dataDir == ""{
		execDir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
		dataDir = execDir+"/data"
	}
	levelDbPath := dataDir+"/leveldb"
	os.MkdirAll(levelDbPath, 0700)
	levelDB, err = leveldb.OpenFile(levelDbPath,nil)
	if err != nil{
		log.Println("init leveldb err:",err)
		os.Exit(0)
	}
}

func GetKeyVal(key []byte) ([]byte,error){
	s, err := levelDB.Get(key, nil)
	return s,err
}

func PutKeyVal(key []byte,val []byte) error{
	err := levelDB.Put(key, val, nil)
	return err
}

func DelKeyVal(key []byte) error{
	return levelDB.Delete(key,nil)
}

type ListStruct struct {
	Key 	string
	Value 	string
}

func GetListByPrefix(key []byte) (data []ListStruct){
	iter := levelDB.NewIterator(util.BytesPrefix(key), nil)
	for iter.Next() {
		data = append(data,ListStruct{Key:string(iter.Key()),Value:string(iter.Value())})
	}
	iter.Release()
	return data
}

func Close(){
	if levelDB != nil{
		levelDB.Close()
	}
}


*/
