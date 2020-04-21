package filequeue

import (
	"fmt"
	"os"
)

// ack 消息数量
func (This *Queue) Ack(n int) (e error){
	l.Lock()
	defer l.Unlock()
	var i int = -1
	var n0 int
	for _,f := range This.unackFileList{
		n0 = f.unackCount - n
		if n0 < 0 {
			n = 0 - n0   // 转成正数，待下一次执行
			i++
		}else{
			f.unackCount = f.unackCount - n
			n = n0
			break
		}
	}
	if i >= 0 {
		list := This.unackFileList[0:i+1]
		This.unackFileList = This.unackFileList[i+1:]
		for _ , fInfo := range list{
			path := This.path+"/"+fmt.Sprint(fInfo.id)+".list"
			//假如路径和正在写的名字一致，则需要先关掉写的句柄再删除
			if This.writeInfo != nil && path == This.writeInfo.name{
				This.writeInfo.fd.Close()
				This.writeInfo = nil
			}
			os.Remove(path)
		}
	}
	return nil
}
