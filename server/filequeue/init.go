package filequeue

import (
	"os"
	"log"
	"io/ioutil"
	"strings"
	"strconv"
	"sync"
	"fmt"
	"bytes"
	"encoding/binary"
)

//存储格式

//int(4) datastring int(4);int(4) datastring int(4)

var l sync.RWMutex
var QueueMap map[string]*Queue

func init()  {
	QueueMap = make(map[string]*Queue,0)
}

type FileInfo struct {
	fd *os.File
	name string
	pos int64
}

type Queue struct{
	sync.RWMutex
	minId int64
	maxId int64
	maxFileSize uint64
	path	string
	readInfo *FileInfo
	writeInfo *FileInfo
	noData	bool
}

func NewQueue(path string) *Queue{
	l.Lock()
	defer l.Unlock()
	if _,ok := QueueMap[path];ok{
		return QueueMap[path];
	}
	Q := &Queue{}
	_, err := os.Stat(path)
	if err != nil {
		err = os.MkdirAll(path,0700)
		if err != nil{
			log.Println("mkdir queue dir err:",err)
			return nil
		}
	}
	maxId := int64(-1)
	minId := int64(0)
	var id0 int64
	//遍历所有path下所有文件,找出最大id
	rd, err := ioutil.ReadDir(path)
	if err == nil {
		for _, fi := range rd {
			if !fi.IsDir() {
				sArr := strings.Split(fi.Name(), ".")
				id0, err = strconv.ParseInt(sArr[0], 10, 64)
				if err == nil {
					if id0 > maxId {
						maxId = id0
					}
					if id0 < minId{
						minId = id0
					}
				}

			}
		}
	}
	Q.path = path
	if maxId == -1{
		Q.noDataInit()
	}else{
		Q.minId = minId
		Q.maxId = maxId
		Q.noData = false
	}
	Q.path = path
	return Q

}


func (This *Queue) noDataInit(){
	This.maxId = -1
	This.minId = 0
	This.readInfo = nil
	This.writeInfo = nil
	This.noData = true
}

func (This *Queue) readInfoInit(){
	fileName := This.path+"/"+fmt.Sprint(This.maxId)+".list"
	fd0,_:=os.OpenFile(fileName,os.O_CREATE|os.O_RDONLY,0700)
	This.readInfo = &FileInfo{
		fd:fd0,
		name:fileName,
		pos:0,
	}
}

func (This *Queue) writeInfoInit(){
	This.maxId = This.maxId+1
	fileName := This.path+"/"+fmt.Sprint(This.maxId)+".list"
	fd0,err:=os.OpenFile(fileName,os.O_RDWR|os.O_CREATE|os.O_APPEND,0700)
	if err!=nil{
		log.Fatal("filequeue writeInfoInit err:",err)
	}
	This.writeInfo = &FileInfo{
		fd:fd0,
		name:fileName,
		pos:0,
	}
}

func Int32ToBytes(n int32) []byte {
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.LittleEndian, n)
	return bytesBuffer.Bytes()
}

func BytesToInt32(b []byte) int32 {
	bytesBuffer := bytes.NewBuffer(b)
	var x int32
	binary.Read(bytesBuffer, binary.LittleEndian, &x)
	return x
}

