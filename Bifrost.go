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
	"flag"
	"fmt"
	"github.com/brokercap/Bifrost/config"
	"github.com/brokercap/Bifrost/manager"
	"github.com/brokercap/Bifrost/plugin"
	"github.com/brokercap/Bifrost/server"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var l sync.Mutex

var logo = `
___         ___                   _   
(  _'\  _  /'___)                 ( )_
| (_) )(_)| (__  _ __   _     ___ | ,_)      Bifrost {$version} {$system}   Listen {$Port}
|  _ <'| || ,__)( '__)/'_'\ /',__)| |                            
| (_) )| || |   | |  ( (_) )\__, \| |_       Pid: {$Pid}
(____/'(_)(_)   (_)  '\___/'(____/'\__)      http://xbifrost.com

                                       `
func printLogo(IpAndPort string){
	var IpAndPort2 string
	//IpAndPort2 = strings.Replace(IpAndPort,"0.0.0.0","127.0.0.1",-1)
	if config.GetConfigVal("Bifrostd","tls") == "true"{
		IpAndPort2 = "https://"+IpAndPort
	}else{
		IpAndPort2 = "http://"+IpAndPort
	}
	logo = strings.Replace(logo,"{$version}",config.VERSION,-1)
	logo = strings.Replace(logo,"{$Port}",IpAndPort2,-1)
	logo = strings.Replace(logo,"{$Pid}",fmt.Sprint(os.Getpid()),-1)
	logo = strings.Replace(logo,"{$system}",fmt.Sprint(runtime.GOARCH),-1)
	fmt.Println(logo)
}

var BifrostConfigFile string
var BifrostDaemon bool
var BifrostPid string
var BifrostDataDir string
var Version bool
var Help bool

func usage() {
	fmt.Fprintf(os.Stderr, `Bifrost version: `+config.VERSION+`
Usage: Bifrost [-hv] [-config ./etc/Bifrost.ini] [-pid Bifrost.pid] [-data_dir dir]

Options:
`)
	flag.PrintDefaults()
}

func main() {
	defer func() {
		server.StopAllChannel()
		doSaveDbInfo()
		server.Close()
		if os.Getppid() == 1 && BifrostPid != ""{
			os.Remove(BifrostPid)
		}
		if err := recover();err != nil{
			log.Println(err)
			log.Println(string(debug.Stack()))
			return
		}
	}()

	flag.StringVar(&BifrostConfigFile,"config", "", "-config")
	flag.StringVar(&BifrostPid,"pid", "", "-pid")
	flag.BoolVar(&BifrostDaemon, "d", false, "-d")
	flag.StringVar(&BifrostDataDir,"data_dir", "", "-data")
	flag.BoolVar(&Version, "v", false, "-v")
	flag.BoolVar(&Help, "h", false, "-h")
	flag.Usage = usage
	flag.Parse()

	if Help{
		flag.Usage()
		os.Exit(0)
	}
	if Version {
		fmt.Println(config.VERSION)
		os.Exit(0)
	}

	if BifrostConfigFile == "" {
		BifrostConfigFile = config.BifrostDir+"/etc/Bifrost.ini"
		log.Println("BifrostConfigFile:",BifrostConfigFile)
	}
	if BifrostConfigFile[0:1] != "/" {
		BifrostConfigFile = config.BifrostDir+BifrostConfigFile
	}
	config.LoadConf(BifrostConfigFile)

	IpAndPort := config.GetConfigVal("Bifrostd","listen")
	if IpAndPort == ""{
		IpAndPort = "0.0.0.0:21036"
	}

	if BifrostDaemon {
		/*
		file := execDir+"/pid.log"
		logFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0700)
		if err != nil {
			panic(err)
		}
		loger := log.New(logFile, "[qSkiptool]",log.LstdFlags | log.Lshortfile | log.LUTC)
		loger.Println("os.Getppid()：",os.Getppid())
		loger.Println("os.Getgid()：",os.Getgid())
		loger.Println("os.Getpid()：",os.Getpid())
		 */
		isDaemo := false
		if os.Getppid() != 1 && runtime.GOOS != "windows" {
			// 因为有一些桌面系统,父进程开了子进程之后,父进程退出之后,并不是由 pid=1 的 systemd 进程接管,可能有些系统给每个桌面帐号重新分配了一下systemd 进程
			// 这里每去判断 一下是不是 systemd 进程名，如果是的话，也认为是父进程被退出了
			cmdString := "ps -ef|grep "+strconv.Itoa(os.Getppid())+" | grep systemd|grep -v grep"
			resultBytes,err := CmdShell(cmdString)
			if err == nil && resultBytes != nil && string(resultBytes) != ""{
				isDaemo = true
			}
		}else {
			isDaemo = true
		}
		if !isDaemo {
			filePath,_:=filepath.Abs(os.Args[0])  //将命令行参数中执行文件路径转换成可用路径
			args:=append([]string{filePath},os.Args[1:]...)
			fmt.Println(filePath)
			fmt.Println(args)
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
	if BifrostDataDir == "" {
		dataDir = config.GetConfigVal("Bifrostd","data_dir")
	}else{
		dataDir = BifrostDataDir
	}

	if dataDir == "" {
		dataDir = config.BifrostDir+"/data"
	}

	if dataDir[0:1] != "/" {
		dataDir = config.BifrostDir+dataDir
	}

	os.MkdirAll(dataDir, 0700)

	config.DataDir = dataDir

	if runtime.GOOS != "windows"{
		if BifrostPid == ""{
			if config.GetConfigVal("Bifrostd","pid") == ""{
				BifrostPid = dataDir+"/Bifrost.pid"
			}else{
				BifrostPid = config.GetConfigVal("Bifrostd","pid")
			}
		}
		WritePid()
	}

	//初始化其他配置
	initParam()

	plugin.DoDynamicPlugin()
	server.InitStorage()

	log.Println("Server started, Bifrost version",config.VERSION)

	doRecovery()

	go manager.Start(IpAndPort)
	ListenSignal()
}

func initParam(){
	var tmp string
	tmp = config.GetConfigVal("Bifrostd","toserver_queue_size")
	if  tmp != ""{
		intA, err := strconv.Atoi(tmp)
		if err == nil && intA > 0{
			config.ToServerQueueSize = intA
		}else{
			log.Println("Bifrost.ini Bifrostd.toserver_queue_size type conversion to int err:",err)
		}
	}

	tmp = config.GetConfigVal("Bifrostd","channel_queue_size")
	if  tmp != ""{
		intA, err := strconv.Atoi(tmp)
		if err == nil && intA > 0{
			config.ChannelQueueSize = intA
		}else{
			log.Println("Bifrost.ini Bifrostd.channel_queue_size type conversion to int err:",err)
		}
	}

	tmp = config.GetConfigVal("Bifrostd","count_queue_size")
	if  tmp != ""{
		intA, err := strconv.Atoi(tmp)
		if err == nil && intA > 0{
			config.CountQueueSize = intA
		}else{
			log.Println("Bifrost.ini Bifrostd.count_queue_size type conversion to int err:",err)
		}
	}

	tmp = config.GetConfigVal("Bifrostd","key_cache_pool_size")
	if  tmp != ""{
		intA, err := strconv.Atoi(tmp)
		if err == nil && intA > 0{
			config.KeyCachePoolSize = intA
		}else{
			log.Println("Bifrost.ini Bifrostd.key_cache_pool_size type conversion to int err:",err)
		}
	}

	if config.GetConfigVal("Bifrostd","file_queue_usable") == "false"{
		config.FileQueueUsable = false
	}

	tmp = config.GetConfigVal("Bifrostd","file_queue_usable_count")
	if  tmp != ""{
		intA, err :=  strconv.ParseUint(tmp,10,32)
		if err == nil && intA > 0{
			config.FileQueueUsableCount = uint32(intA)
		}else{
			log.Println("Bifrost.ini Bifrostd.file_queue_usable_count type conversion to uint32 err:",err)
		}
	}

	tmp = config.GetConfigVal("Bifrostd","file_queue_usable_count_time_diff")
	if  tmp != ""{
		intA, err :=  strconv.ParseInt(tmp,10,64)
		if err == nil && intA > 0{
			config.FileQueueUsableCountTimeDiff = intA
		}else{
			log.Println("Bifrost.ini Bifrostd.file_queue_usable_count_time_diff type conversion to int64 err:",err)
		}
	}

	tmp = config.GetConfigVal("Bifrostd","plugin_commit_timeout")
	if  tmp != ""{
		intA, err := strconv.Atoi(tmp)
		if err == nil && intA > 0{
			config.PluginCommitTimeOut = intA
		}else{
			log.Println("Bifrost.ini Bifrostd.plugin_commit_timeout type conversion to int err:",err)
		}
	}

	tmp = config.GetConfigVal("Bifrostd","plugin_sync_retry_time")
	if  tmp != ""{
		intA, err := strconv.Atoi(tmp)
		if err == nil && intA > 0{
			config.PluginSyncRetrycTime = intA
		}else{
			log.Println("Bifrost.ini Bifrostd.plugin_sync_retry_time type conversion to int err:",err)
		}
	}

	initTLSParam()
}

func initTLSParam(){
	var path string
	var tlsKeyFile string = config.GetConfigVal("Bifrostd","tls_key_file")
	var tlsCrtFile string = config.GetConfigVal("Bifrostd","tls_crt_file")

	var checkFileStat = func(pathDir,file string) error{
		_, err := os.Stat(pathDir+"/"+file)
		return err
	}

	var setTLSConfig = func() {
		config.TLSServerKeyFile = path+"/"+tlsKeyFile
		config.TLSServerCrtFile = path+"/"+tlsCrtFile
		config.TLS = true
	}

	if config.GetConfigVal("Bifrostd","tls") == "true"{
		for {
			// 相对于 Bifrost 二进制可执行文件的路径
			path,_ = filepath.Abs(filepath.Dir(os.Args[0]))
			if checkFileStat(path,tlsKeyFile) == nil &&  checkFileStat(path,tlsCrtFile) == nil {
				setTLSConfig()
				break
			}
			// 相对于 Bifost 二进制可执行文件的父目录的路径
			path = filepath.Dir(path)
			if checkFileStat(path,tlsKeyFile) == nil &&  checkFileStat(path,tlsCrtFile) == nil {
				setTLSConfig()
				break
			}
			// 相对于 Bifrost.ini 配置文件的路径
			path,_ = filepath.Abs(filepath.Dir(BifrostConfigFile))
			BifrostConfigFilePath := path
			if checkFileStat(path,tlsKeyFile) == nil &&  checkFileStat(path,tlsCrtFile) == nil {
				setTLSConfig()
				break
			}
			// 绝对路径
			path = ""
			if checkFileStat(path,tlsKeyFile) == nil &&  checkFileStat(path,tlsCrtFile) == nil {
				setTLSConfig()
				break
			}
			log.Println("tls_key_file:",BifrostConfigFilePath+"/"+tlsKeyFile," or tls_crt_file: ",BifrostConfigFilePath+"/"+tlsCrtFile, " not exsit")
			return
		}
	}
}


func initLog(){
	log_dir := config.GetConfigVal("Bifrostd","log_dir")
	if log_dir == "" {
		log.Println("no config [ Bifrostd log_dir ] ")
		log_dir = config.BifrostDir+"/logs"
		log.Println("log_dir default:",log_dir)
	}
	if log_dir[0:1] != "/"{
		log_dir = config.BifrostDir+log_dir
	}
	os.MkdirAll(log_dir,0700)
	t := time.Now().Format("2006-01-02")
	LogFileName := log_dir+"/Bifrost_"+t+".log"
	f, err := os.OpenFile(LogFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0700) //打开文件
	if err != nil{
		log.Println("log init error:",err)
	}
	log.SetOutput(f)
	fmt.Println("log input to",LogFileName)
}

func WritePid(){
	var err error
	var pidFileFd *os.File
	pidFileFd, err = os.OpenFile(BifrostPid, os.O_CREATE|os.O_RDWR, 0700) //打开文件
	if err !=nil{
		log.Println("Open BifrostPid Error; File:",BifrostPid,"; Error:",err)
		os.Exit(1)
		return
	}
	defer pidFileFd.Close()
	pidContent, err2 := ioutil.ReadAll(pidFileFd)
	if string(pidContent) != ""{
		ExitBool := true
		cmdString := "ps -ef|grep "+string(pidContent)+" | grep "+filepath.Base(os.Args[0]+"|grep -v grep")
		resultBytes,err := CmdShell(cmdString)
		// err 不为 nil 则代表没有grep 到进程，可以认为有可能被 kill -9 等操作了
		if err != nil &&  resultBytes != nil{
			ExitBool = false
		}else{
			log.Println(cmdString," result:",string(resultBytes)," err:",err,)
		}
		if ExitBool {
			log.Println("Birostd server quit without updating PID file ; File:", BifrostPid, "; Error:", err2)
			os.Exit(1)
		}
	}
	os.Truncate(BifrostPid, 0)
	pidFileFd.Seek(0,0)
	io.WriteString(pidFileFd,fmt.Sprint(os.Getpid()))
}

func CmdShell(cmdString string)([]byte,error){
	switch runtime.GOOS {
		case "linux","darwin","freebsd":
		cmd := exec.Command("/bin/bash", "-c", cmdString)
		return cmd.Output()
		break
	default:
		break
	}
	return nil,fmt.Errorf(runtime.GOOS+" not supported")
}

func doSaveDbInfo(){
	if os.Getppid() != 1 && BifrostDaemon{
		return
	}
	server.DoSaveSnapshotData()
}


func doRecovery(){
	server.DoRecoverySnapshotData()
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
		os.Remove(BifrostPid)
		server.Close()
		os.Exit(0)
	}
}
