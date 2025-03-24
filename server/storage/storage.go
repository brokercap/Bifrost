package storage

import (
	"github.com/brokercap/Bifrost/config"
	"github.com/brokercap/Bifrost/xdb"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"
)

var xdbClient *xdb.Client
var xdbErr error

const DEFAULT_TABLE = "bifrost"

var metaStorageType string
var path string
var dbSourceKey []byte

func init() {}

func InitStorage() {
	metaStorageType = strings.ToLower(config.GetConfigVal("Bifrostd", "meta_storage_type"))
	switch metaStorageType {
	case "redis":
		path = config.GetConfigVal("Bifrostd", "meta_storage_path")
		if config.GetConfigVal("Bifrostd", "cluster_name") == "" {
			log.Println("config cluster_name is empty!")
			os.Exit(1)
		}
		break
	default:
		metaStorageType = "leveldb"
		path = config.DataDir + "/leveldb"
		break
	}
	xdbClient, xdbErr = xdb.NewClient(metaStorageType, path)
	if xdbErr != nil {
		log.Println(xdbErr)
		os.Exit(1)
	}
	xdbClient.SetPrefix(config.GetConfigVal("Bifrostd", "cluster_name"))

	dbSourceKey = []byte("dbSourceData")
}

func GetKeyVal(key []byte) (data []byte, err error) {
	for i := 0; i < 3; i++ {
		data, err = xdbClient.GetKeyValBytes(DEFAULT_TABLE, string(key))
		if err == nil {
			break
		}
		time.Sleep(time.Duration(1) * time.Second)
	}
	return
}

func PutKeyVal(key []byte, val []byte) (err error) {
	for i := 0; i < 3; i++ {
		err = xdbClient.PutKeyValBytes(DEFAULT_TABLE, string(key), val)
		if err == nil {
			break
		}
		time.Sleep(time.Duration(1) * time.Second)
	}
	return
}

func DelKeyVal(key []byte) (err error) {
	for i := 0; i < 3; i++ {
		err = xdbClient.DelKeyVal(DEFAULT_TABLE, string(key))
		if err == nil {
			break
		}
		time.Sleep(time.Duration(1) * time.Second)
	}
	return
}

type ListStruct struct {
	Key   string
	Value string
}

func GetListByPrefix(key []byte) (data []ListStruct) {
	for i := 0; i < 3; i++ {
		dataList, err := xdbClient.GetListByKeyPrefix(DEFAULT_TABLE, string(key), nil)
		if err != nil {
			time.Sleep(time.Duration(1) * time.Second)
			continue
		}
		for _, v := range dataList {
			data = append(data, ListStruct{
				Key:   v.Key,
				Value: v.Value,
			})
		}
		break
	}
	return data
}

func Close() {
	if xdbClient != nil {
		xdbClient.Close()
	}
}

func GetDBInfo() (data []byte, err error) {
	switch metaStorageType {
	case "redis":
		data, err = GetKeyVal(dbSourceKey)
		break
	default:
		var DataFile string = config.DataDir + "/db.Bifrost"
		//DataTmpFile = dataDir+"/db.Bifrost.tmp"
		var fi *os.File
		fi, err = os.Open(DataFile)
		if err != nil {
			return
		}
		defer fi.Close()
		data, err = ioutil.ReadAll(fi)
		if err != nil {
			return
		}
	}
	return
}

func SaveDBInfo(data []byte) (err error) {
	switch metaStorageType {
	case "redis":
		err = PutKeyVal(dbSourceKey, data)
		break
	default:
		var DataFile string = config.DataDir + "/db.Bifrost"
		var DataTmpFile string = config.DataDir + "/db.Bifrost.tmp"
		var f *os.File
		f, err = os.OpenFile(DataTmpFile, os.O_CREATE|os.O_RDWR, 0700) //打开文件
		if err != nil {
			log.Println("open file error:", err)
			return
		}
		_, err = io.WriteString(f, string(data)) //写入文件(字符串)
		if err != nil {
			f.Close()
			log.Printf("save data to file error:%s, data:%s \r\n", err, string(data))
			return
		}
		f.Close()
		err = os.Rename(DataTmpFile, DataFile)
		if err != nil {
			log.Println("doSaveDbInfo os.Rename err:", err)
		}
		break
	}
	return
}
