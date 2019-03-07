/*
Copyright [2018] [jc3wish]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package main

import (
	"log"

	"github.com/jc3wish/Bifrost/server"
	"github.com/jc3wish/Bifrost/toserver"
	"github.com/jc3wish/Bifrost/manager"
	"github.com/jc3wish/Bifrost/config"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"
	"encoding/json"
	"io"
	"sync"
	"io/ioutil"
	"fmt"
	"strings"
	"path/filepath"
	"runtime"
	"runtime/debug"
)

type recovery struct {
	Version string
	ToServer *json.RawMessage
	DbInfo *json.RawMessage
}

type recoveryData struct {
	Version string
	ToServer interface{}
	DbInfo interface{}
}

var l sync.Mutex

var DataFile string
var DataTmpFile string

var logo = `
___         ___                   _   
(  _'\  _  /'___)                 ( )_
| (_) )(_)| (__  _ __   _     ___ | ,_)      Bifrost {$version} {$system}   Listen {$Port}
|  _ <'| || ,__)( '__)/'_'\ /',__)| |                            
| (_) )| || |   | |  ( (_) )\__, \| |_       Pid: {$Pid}
(____/'(_)(_)   (_)  '\___/'(____/'\__)      http://xbifrost.com

                                       `
func printLogo(IpAndPort string){
	logo = strings.Replace(logo,"{$version}",config.VERSION,-1)
	logo = strings.Replace(logo,"{$Port}",IpAndPort,-1)
	logo = strings.Replace(logo,"{$Pid}",fmt.Sprint(os.Getpid()),-1)
	logo = strings.Replace(logo,"{$system}",fmt.Sprint(runtime.GOARCH),-1)
	fmt.Println(logo)
}

var BifrostConfigFile *string
var BifrostDaemon *string
var BifrostPid *string
var BifrostDataDir *string

func main() {
	defer func() {
		server.StopAllChannel()
		doSaveDbInfo()
		if os.Getppid() == 1 && *BifrostPid != ""{
			os.Remove(*BifrostPid)
		}
		if err := recover();err != nil{
			log.Println(debug.Stack())
			return
		}
	}()
	execDir, _ := filepath.Abs(filepath.Dir(os.Args[0]))

	DataFile  = ""
	DataTmpFile = ""

	BifrostConfigFile = flag.String("config", "", "Bifrost config file path")
	BifrostPid = flag.String("pid", "", "pid file path")
	BifrostDaemon = flag.String("d", "false", "true|false, default(false)")
	BifrostDataDir = flag.String("data_dir", "", "db.Bifrost data dir")
	flag.Parse()
	if *BifrostConfigFile == ""{
		*BifrostConfigFile = execDir+"/etc/Bifrost.ini"
		log.Println("*BifrostConfigFile:",*BifrostConfigFile)
	}
	config.LoadConf(*BifrostConfigFile)

	IpAndPort := config.GetConfigVal("Bifrostd","listen")
	if IpAndPort == ""{
		IpAndPort = "0.0.0.0:21036"
	}


	if *BifrostDaemon == "true"{
		if os.Getppid() != 1{
			filePath,_:=filepath.Abs(os.Args[0])  //将命令行参数中执行文件路径转换成可用路径
			args:=append([]string{filePath},os.Args[1:]...)
			os.StartProcess(filePath,args,&os.ProcAttr{Files:[]*os.File{os.Stdin,os.Stdout,os.Stderr}})
			return
		}else{
			printLogo(IpAndPort)
			initLog()
			fmt.Printf("Please press the `Enter`\r")
		}
	}else{
		printLogo(IpAndPort)
	}

	var dataDir string
	if *BifrostDataDir == ""{
		dataDir = config.GetConfigVal("Bifrostd","data_dir")
	}else{
		dataDir = *BifrostDataDir
	}
	if dataDir == ""{
		dataDir = execDir+"/data"
	}
	os.MkdirAll(dataDir, 0777)

	if runtime.GOOS != "windows"{
		if *BifrostPid == ""{
			if config.GetConfigVal("Bifrostd","pid") == ""{
				*BifrostPid = dataDir+"/Bifrost.pid"
			}else{
				*BifrostPid = config.GetConfigVal("Bifrostd","pid")
			}
		}
		WritePid()
	}

	log.Println("Server started, Bifrost version",config.VERSION)


	DataFile = dataDir+"/db.Bifrost"
	DataTmpFile = dataDir+"/db.Bifrost.tmp"

	doRecovery()

	go TimeSleepDoSaveInfo()
	go manager.Start(IpAndPort)
	ListenSignal()
}

func initLog(){
	log_dir := config.GetConfigVal("Bifrostd","log_dir")
	if log_dir == ""{
		log.Println("no config [ Bifrostd log_dir ] ")
		log_dir, _ = filepath.Abs(filepath.Dir(os.Args[0]))
		log_dir += "/logs"
		log.Println("log_dir default:",log_dir)
	}
	os.MkdirAll(log_dir,0777)
	t := time.Now().Format("2006-01-02")
	LogFileName := log_dir+"/Bifrost_"+t+".log"
	f, err := os.OpenFile(LogFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0777) //打开文件
	if err != nil{
		log.Println("log init error:",err)
	}
	log.SetOutput(f)
	fmt.Println("log input to",LogFileName)
}

func WritePid(){
	f, err2 := os.OpenFile(*BifrostPid, os.O_CREATE|os.O_RDWR, 0777) //打开文件
	if err2 !=nil{
		log.Println("Open BifrostPid Error; File:",*BifrostPid,"; Error:",err2)
		os.Exit(1)
		return
	}
	pidContent, err2 := ioutil.ReadAll(f)
	if string(pidContent) != ""{
		log.Println("Birostd server quit without updating PID file ; File:",*BifrostPid,"; Error:",err2)
		os.Exit(1)
	}
	defer f.Close()
	io.WriteString(f, fmt.Sprint(os.Getpid()))
}

func TimeSleepDoSaveInfo(){
	for {
		time.Sleep(5 * time.Second)
		doSaveDbInfo()
		}
}

func doSaveDbInfo(){
	if os.Getppid() != 1 && *BifrostDaemon == "true"{
		return
	}
	l.Lock()
	defer func(){
		l.Unlock()
		if err :=recover();err!=nil{
			log.Println(err)
		}
	}()
	data := recoveryData{
		Version:config.VERSION,
		ToServer:toserver.SaveToServerData(),
		DbInfo:server.SaveDBInfoToFileData(),
	}
	b,_:= json.Marshal(data)
	f, err2 := os.OpenFile(DataTmpFile, os.O_CREATE|os.O_RDWR, 0777) //打开文件
	if err2 !=nil{
		log.Println("open file error:",err2)
		return
	}
	defer f.Close()
	_, err1 := io.WriteString(f, string(b)) //写入文件(字符串)
	if err1 != nil {
		log.Printf("save data to file error:%s, data:%s \r\n",err1,string(b))
		return
	}
	os.Rename(DataTmpFile,DataFile)
}


func doRecovery(){
	fi, err := os.Open(DataFile)
	if err != nil {
		return
	}
	defer fi.Close()
	fd, err := ioutil.ReadAll(fi)
	if err != nil {
		return
	}
	if string(fd) == ""{
		return
	}
	var data recovery
	errors := json.Unmarshal(fd,&data)
	if errors != nil{
		log.Printf("recovery error:%s, data:%s \r\n",errors,string(fd))
	}
	if string(*data.ToServer) != "{}"{
		toserver.Recovery(data.ToServer)
	}
	if string(*data.DbInfo) != "{}"{
		server.Recovery(data.DbInfo)
	}
}

func ListenSignal(){
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGINT)
	for sig := range signals {
		if sig == nil{
			continue
		}
		server.StopAllChannel()
		doSaveDbInfo()
		os.Remove(*BifrostPid)
		os.Exit(0)
	}
}
