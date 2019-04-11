package storage

import (
"github.com/syndtr/goleveldb/leveldb"
"github.com/syndtr/goleveldb/leveldb/util"
"github.com/jc3wish/Bifrost/config"
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
	levelDB, err = leveldb.OpenFile(levelDbPath, nil)
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

func GetListByPrefix(key []byte) [][][]byte{
	data := make([][][]byte,0)
	iter := levelDB.NewIterator(util.BytesPrefix(key), nil)
	for iter.Next() {
		tmp := make([][]byte,2)
		tmp[0] = iter.Key()
		tmp[1] = iter.Value()
		data = append(data,tmp)
	}
	iter.Release()
	return data
}


