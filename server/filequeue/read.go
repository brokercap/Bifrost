package filequeue

import (
	"os"
	"fmt"
	"path/filepath"
)

func (This *Queue) Pop() (content string,e error){
	This.Lock()
	defer This.Unlock()
	if This.noData == true{
		return "",nil
	}
	if This.readInfo == nil{
		This.readInfoInit()
	}
	var n int
	var l int32
	b := make([]byte,4)
	_,e =This.readInfo.fd.Read(b)
	if e != nil{
		return
	}
	l = BytesToInt32(b)
	//这里加5,是多加载一个下一条数据的字节,判断是否还存在 下一条数据
	c := make([]byte, l+5)
	n, e = This.readInfo.fd.Read(c)
	if e != nil {
		return
	}
	//假如实际读取数据大小,小于 l+5,则代表当前队列文件没有下一条数据了,可以把这个文件给删除掉了
	if n < int(l+5) {
		if This.maxId <= This.minId {
			if This.writeInfo!=nil{
				This.writeInfo.fd.Close()
			}
		}
		This.readInfo.fd.Close()
		os.Remove(This.readInfo.name)
		content = string(c[0:n-4])
		This.readInfo = nil
		This.writeInfo = nil
		if This.maxId <= This.minId {
			This.noDataInit()
		}
	} else {
		content = string(c[0:n-5])
		This.readInfo.pos += int64(n)
		//This.readInfo.fd.Seek(This.readInfo.pos, 0)
	}

	return
}




//获取最后一条数据
func (This *Queue) ReadLast() (content string,e error){
	l.Lock()
	defer l.Unlock()
	if This.maxId == -1{
		return
	}
	if This.writeInfo != nil{
		This.writeInfo.fd.Close()
		This.writeInfo = nil
	}
	fileName := This.path+"/"+fmt.Sprint(This.maxId)+".list"
	fileSize := getFileSize(fileName)
	var fd *os.File
	fd,e=os.OpenFile(fileName,os.O_RDONLY,0700)
	if e!=nil{
		return
	}
	fd.Seek(fileSize - 4,0)
	b := make([]byte,4)
	_, e = fd.Read(b)
	if e!=nil{
		return
	}
	l := BytesToInt32(b)
	fd.Seek(fileSize - 4 - int64(l),0)
	c := make([]byte,l)
	fd.Read(c)
	content = string(c)
	return
}

func getFileSize(filename string) int64 {
	var result int64
	filepath.Walk(filename, func(path string, f os.FileInfo, err error) error {
		result = f.Size()
		return nil
	})
	return result
}