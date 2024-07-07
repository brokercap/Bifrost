package filequeue

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func (This *Queue) Pop() (content []byte, e error) {
	This.Lock()
	defer This.Unlock()
	if This.readInfo == nil {
		This.readInfoInit()
	}
	if This.readInfo == nil {
		return nil, nil
	}
	var n int
	var l int32
	b := make([]byte, 4)
	_, e = This.readInfo.fd.Read(b)
	if e != nil {
		return
	}
	l = BytesToInt32(b)
	//这里加5,是多加载一个下一条数据的字节,判断是否还存在 下一条数据
	c := make([]byte, l+5)
	n, e = This.readInfo.fd.Read(c)
	if e != nil {
		if e == io.EOF {
			e = nil
		} else {
			return nil, e
		}
	}
	if n == int(l)+5 {
		content = c[0 : n-5]
	} else if n == int(l)+4 {
		content = c[0 : n-4]
		//log.Println("content:",string(content))
		This.readInfo.fd.Close()
		This.readInfo.pos = 0
		This.readInfo = nil
		This.unackFileList[len(This.unackFileList)-1].allInMemory = true
	} else {
		return nil, fmt.Errorf("read file err,fileName:%s", This.readInfo.name)
	}
	This.unackFileList[len(This.unackFileList)-1].unackCount++
	This.unackFileList[len(This.unackFileList)-1].totalCount++
	return
}

// 获取最后一条数据
func (This *Queue) ReadLast() (content []byte, e error) {
	This.Lock()
	defer This.Unlock()
	if This.maxId == -1 {
		return
	}
	if This.writeInfo != nil {
		This.writeInfo.fd.Close()
		This.writeInfo = nil
	}
	fileName := This.path + "/" + fmt.Sprint(This.maxId) + ".list"
	fileSize := getFileSize(fileName)
	var fd *os.File
	fd, e = os.OpenFile(fileName, os.O_RDONLY, 0700)
	if e != nil {
		return
	}
	fd.Seek(fileSize-4, 0)
	b := make([]byte, 4)
	_, e = fd.Read(b)
	if e != nil && e != io.EOF {
		return
	}
	l := BytesToInt32(b)
	fd.Seek(fileSize-4-int64(l), 0)
	c := make([]byte, l)
	fd.Read(c)
	return c, e
}

func getFileSize(filename string) int64 {
	var result int64
	filepath.Walk(filename, func(path string, f os.FileInfo, err error) error {
		result = f.Size()
		return nil
	})
	return result
}
