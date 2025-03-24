package user

import (
	"github.com/brokercap/Bifrost/config"
	"log"
	"os"
	"strings"
)

func getLogFile() string {
	return config.BifrostLogDir + "/login.log"
}

func appendLoginLog(format string, v ...interface{}) {
	f, err := os.OpenFile(getLogFile(), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0700) //打开文件
	if err != nil {
		log.Println("open login file error:", err, getLogFile())
		return
	}
	defer f.Close()
	log.New(f, "", log.Ldate|log.Ltime|log.Lshortfile).Printf(format, v...)
}

func GetLastLoginLog() (logInfo string, err error) {
	filename := getLogFile()
	var fileHandle *os.File
	fileHandle, err = os.Open(filename)
	if err != nil {
		return
	}
	defer fileHandle.Close()
	stat, _ := fileHandle.Stat()

	filesize := stat.Size()
	var fileSeek int64
	fileSeek = filesize - 1*8*1024
	if fileSeek < 0 {
		fileSeek = 0
	}
	fileHandle.Seek(fileSeek, 0)
	buffer := make([]byte, 16*1024)
	var n int
	n, err = fileHandle.Read(buffer)
	if err != nil {
		return
	}
	buffer = buffer[0:n]
	logInfo = strings.Replace(string(buffer), "\r\n", "</br>", -1)
	logInfo = strings.Replace(logInfo, "\n", "</br>", -1)
	logInfo = strings.Replace(logInfo, "\r", "</br>", -1)
	return
}
