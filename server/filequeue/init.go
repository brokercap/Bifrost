package filequeue

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

//存储格式

//int(4) datastring int(4);int(4) datastring int(4)

var l sync.RWMutex
var QueueMap map[string]*Queue

func init() {
	QueueMap = make(map[string]*Queue, 0)
}

type FileInfo struct {
	fd   *os.File
	name string
	pos  int64
}

type unackFileInfo struct {
	id          int64 // 文件编号
	unackCount  int   // unack 数量
	allInMemory bool  // 全部数据已经加载到内存
	totalCount  int   // 整个文件已经加载到内存消息条数
}

type Queue struct {
	sync.RWMutex
	minId         int64  // 最小文件
	maxId         int64  // 当前最大文件，当 -1 的时候，代表整个目录为空
	maxFileSize   uint64 //
	path          string // 文件夹路径
	readInfo      *FileInfo
	writeInfo     *FileInfo
	fileCount     int              // 文件数量
	unackFileList []*unackFileInfo // 已经被加载到内存了的文件信息
}

type QueueInfo struct {
	sync.RWMutex
	MinId         int64           // 最小文件
	MaxId         int64           // 当前最大文件，当 -1 的时候，代表整个目录为空
	Path          string          // 文件夹路径
	FileCount     int             // 文件数量
	UnackFileList []UnackFileInfo // 已加载到内存的文件信息
}

type UnackFileInfo struct {
	Id          int64 // 文件编号
	UnackCount  int   // unack 数量
	AllInMemory bool  // 全部数据已经加载到内存
	TotalCount  int   // 整个文件已经加载到内存消息条数
}

func NewQueue(path string) *Queue {
	l.Lock()
	defer l.Unlock()
	if _, ok := QueueMap[path]; ok {
		return QueueMap[path]
	}
	Q := &Queue{}
	//这里为什么要加 /tmp/ 结尾来创建，是因为 在实际 测试中centos7  go1.14.2 版本中数字结尾的目录，最后一层个位数字名字目录，可能是不会被创建的
	err := os.MkdirAll(path+"/tmp/", 0700)
	//os.MkdirAll(path+"/a/",0700)
	//os.MkdirAll("/data/bifrost/filequeue/mysqlLocalTest/test/binlog_field_test/a/",0700)
	//log.Println("path:",path+"/")
	if err != nil {
		log.Println("mkdir queue dir err:", err)
		return nil
	}
	maxId := int64(-1)
	minId := int64(-1)
	fileCount := 0
	var id0 int64
	//遍历所有path下所有文件,找出最大id
	rd, err := ioutil.ReadDir(path)
	if err == nil {
		for _, fi := range rd {
			if !fi.IsDir() {
				sArr := strings.Split(fi.Name(), ".")
				//后缀是.list 才是队列存储文件
				if sArr[len(sArr)-1] == "list" {
					fileCount++
					id0, err = strconv.ParseInt(sArr[0], 10, 64)
					if err == nil {
						if id0 > maxId {
							maxId = id0
						}
						if id0 < minId || minId == -1 {
							minId = id0
						}
					}
				}

			}
		}
	}
	Q.path = path
	if maxId == -1 {
		Q.noDataInit()
	} else {
		Q.minId = minId
		Q.maxId = maxId
		Q.fileCount = fileCount
	}
	Q.path = path
	Q.unackFileList = make([]*unackFileInfo, 0)
	QueueMap[path] = Q
	return Q

}

func (This *Queue) noDataInit() {
	This.maxId = -1
	This.minId = 0
	This.fileCount = 0
	This.readInfo = nil
	This.writeInfo = nil
}

func (This *Queue) GetInfo() QueueInfo {
	This.Lock()
	defer This.Unlock()
	FileList := make([]UnackFileInfo, 0)
	for _, fileInfo := range This.unackFileList {
		FileList = append(FileList, UnackFileInfo{
			Id:          fileInfo.id,
			UnackCount:  fileInfo.unackCount,
			AllInMemory: fileInfo.allInMemory,
			TotalCount:  fileInfo.totalCount,
		})
	}
	return QueueInfo{
		MinId:         This.minId,
		MaxId:         This.maxId,
		Path:          This.path,
		FileCount:     This.fileCount,
		UnackFileList: FileList,
	}
}

func (This *Queue) readInfoInit() {
	fileName := This.path + "/" + fmt.Sprint(This.minId) + ".list"
	fd0, err := os.OpenFile(fileName, os.O_RDONLY, 0700)
	if err != nil {
		This.readInfo = nil
		return
	}
	unackFile := &unackFileInfo{
		id:          This.minId,
		unackCount:  0,
		allInMemory: false,
	}
	This.minId += 1
	This.unackFileList = append(This.unackFileList, unackFile)
	This.readInfo = &FileInfo{
		fd:   fd0,
		name: fileName,
		pos:  0,
	}
}

func (This *Queue) writeInfoInit() {
	This.maxId = This.maxId + 1
	fileName := This.path + "/" + fmt.Sprint(This.maxId) + ".list"
	fd0, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0700)
	if err != nil {
		log.Fatal("filequeue writeInfoInit err:", err)
	}
	This.fileCount++
	This.writeInfo = &FileInfo{
		fd:   fd0,
		name: fileName,
		pos:  0,
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
