package leveldb

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/brokercap/Bifrost/xdb/driver"
	"os"
	"log"
	"fmt"
)

const VERSION  = "v1.1.0"

type MyConn struct {}

func (MyConn *MyConn) Open(uri string) (driver.XdbDriver,error){
	return newConn(uri)
}

func newConn(path string) (*Conn,error){
	if path == ""{
		return nil,fmt.Errorf("path error")
	}
	f := &Conn{
		path:path,
	}
	err := f.connect()
	if err != nil{
		return nil,err
	}
	return f,nil
}

type Conn struct {
	path    string
	err  	error
	levelDB *leveldb.DB
}

func (This *Conn) connect() error{
	os.MkdirAll(This.path, 0700)
	This.levelDB, This.err = leveldb.OpenFile(This.path, nil)
	if This.err != nil{
		log.Println("sdfsdfsd",This.err)
	}
	return This.err
}

func (This *Conn) Close() (error){
	This.levelDB.Close()
	return nil
}

func (This *Conn) GetKeyVal(key []byte) ([]byte,error){
	s, err := This.levelDB.Get(key, nil)
	return s,err
}

func (This *Conn) PutKeyVal(key []byte,val []byte) error{
	err := This.levelDB.Put(key, val, nil)
	return err
}

func (This *Conn) DelKeyVal(key []byte) error{
	return This.levelDB.Delete(key,nil)
}

func (This *Conn) GetListByKeyPrefix(key []byte) ([]string,error){
	data := make([]string,0)
	iter := This.levelDB.NewIterator(util.BytesPrefix(key), nil)
	for iter.Next() {
		//tmp := make([][]byte,1)
		//tmp[0] = iter.Key()
		//tmp[1] = iter.Value()
		//log.Println("tmp1:",string(iter.Value()))
		data = append(data,string(iter.Value()))
	}
	iter.Release()
	return data,nil
}