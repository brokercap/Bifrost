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
	manager "github.com/brokercap/Bifrost/admin"
	"github.com/brokercap/Bifrost/config"
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
import _ "github.com/brokercap/Bifrost/plugin/load"
import _ "github.com/brokercap/Bifrost/input"

var l sync.Mutex
var saveDbInfoStatus bool

var logo = `
___         ___                   _   
(  _'\  _  /'___)                 ( )_
| (_) )(_)| (__  _ __   _     ___ | ,_)      Bifrost {$version} {$system}   Listen {$Port}
|  _ <'| || ,__)( '__)/'_'\ /',__)| |                            
| (_) )| || |   | |  ( (_) )\__, \| |_       Pid: {$Pid}
(____/'(_)(_)   (_)  '\___/'(____/'\__)      http://xbifrost.com
纪念2022.08.28认识Monty并合影

                                       `

func printLogo() {
	var IpAndPort2 string
	//IpAndPort2 = strings.Replace(IpAndPort,"0.0.0.0","127.0.0.1",-1)
	if config.TLS {
		IpAndPort2 = "https://" + config.Listen
	} else {
		IpAndPort2 = "http://" + config.Listen
	}
	logo = strings.Replace(logo, "{$version}", config.VERSION, -1)
	logo = strings.Replace(logo, "{$Port}", IpAndPort2, -1)
	logo = strings.Replace(logo, "{$Pid}", fmt.Sprint(os.Getpid()), -1)
	logo = strings.Replace(logo, "{$system}", fmt.Sprint(runtime.GOARCH), -1)
	fmt.Println(logo)
}

var BifrostDaemon bool

func usage() {
	fmt.Fprintf(os.Stderr, `Bifrost version: `+config.VERSION+`
Usage: Bifrost [-hv] [-config ./etc/Bifrost.ini] [-pid Bifrost.pid] [-data_dir dir]

Options:
`)
	flag.PrintDefaults()
}

func main() {
	var BifrostPid string
	var BifrostDataDir string
	var Version bool
	var Help bool
	defer func() {
		doSeverDbInfoFun()
		if os.Getppid() == 1 && config.BifrostPidFile != "" {
			os.Remove(config.BifrostPidFile)
		}
		if err := recover(); err != nil {
			log.Println(err)
			log.Println(string(debug.Stack()))
			return
		}
	}()

	flag.StringVar(&config.BifrostConfigFile, "config", "", "-config")
	flag.StringVar(&BifrostPid, "pid", "", "-pid")
	flag.BoolVar(&BifrostDaemon, "d", false, "-d")
	flag.StringVar(&BifrostDataDir, "data_dir", "", "-data_dir")
	flag.BoolVar(&Version, "v", false, "-v")
	flag.BoolVar(&Help, "h", false, "-h")
	flag.Usage = usage
	flag.Parse()

	if Help {
		flag.Usage()
		os.Exit(0)
	}
	if Version {
		fmt.Println(config.VERSION)
		os.Exit(0)
	}

	if BifrostDaemon {
		var isDaemoProcess bool = false
		if os.Getppid() != 1 && runtime.GOOS != "windows" {
			// 因为有一些桌面系统,父进程开了子进程之后,父进程退出之后,并不是由 pid=1 的 systemd 进程接管,可能有些系统给每个桌面帐号重新分配了一下systemd 进程
			// 这里每去判断 一下是不是 systemd 进程名，如果是的话，也认为是父进程被退出了
			cmdString := "ps -ef|grep " + strconv.Itoa(os.Getppid()) + " | grep systemd|grep -v grep"
			resultBytes, err := CmdShell(cmdString)
			if err == nil && resultBytes != nil && string(resultBytes) != "" {
				isDaemoProcess = true
			}
		} else {
			isDaemoProcess = true
		}
		if !isDaemoProcess {
			filePath, _ := filepath.Abs(os.Args[0]) //将命令行参数中执行文件路径转换成可用路径
			args := append([]string{filePath}, os.Args[1:]...)
			os.StartProcess(filePath, args, &os.ProcAttr{Files: []*os.File{os.Stdin, os.Stdout, os.Stderr}})
			return
		}
		if config.BifrostPidFile == "" {
			config.BifrostPidFile = "./Bifrost.pid"
		}
	}

	config.LoadConf(config.BifrostConfigFile)
	if BifrostDataDir != "" {
		config.SetConfigVal("Bifrostd", "data_dir", BifrostDataDir)
	}
	if BifrostPid != "" {
		config.SetConfigVal("Bifrostd", "pid", BifrostPid)
	}
	config.InitParam()

	if !BifrostDaemon {
		printLogo()
	} else {
		printLogo()
		initLog()
		fmt.Printf("Please press the `Enter`\r")
	}

	os.MkdirAll(config.DataDir, 0700)

	WritePid()

	plugin.DoDynamicPlugin()
	server.InitStorage()

	log.Println("Server started, Bifrost version", config.VERSION)

	doRecovery()

	go manager.Start()
	ListenSignal()
}

func initLog() {
	os.MkdirAll(config.BifrostLogDir, 0700)
	t := time.Now().Format("2006-01-02")
	LogFileName := config.BifrostLogDir + "/Bifrost_" + t + ".log"
	f, err := os.OpenFile(LogFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0700) //打开文件
	if err != nil {
		log.Println("log init error:", err)
	}
	log.SetOutput(f)
	fmt.Println("log input to", LogFileName)
}

func WritePid() {
	if config.BifrostPidFile == "" {
		return
	}
	var err error
	var pidFileFd *os.File
	pidFileFd, err = os.OpenFile(config.BifrostPidFile, os.O_CREATE|os.O_RDWR, 0700) //打开文件
	if err != nil {
		log.Println("Open BifrostPid Error; File:", config.BifrostPidFile, "; Error:", err)
		os.Exit(1)
		return
	}
	defer pidFileFd.Close()
	pidContent, err2 := ioutil.ReadAll(pidFileFd)
	if string(pidContent) != "" {
		ExitBool := true
		cmdString := "ps -ef|grep " + string(pidContent) + " | grep " + filepath.Base(os.Args[0]+"|grep -v grep")
		resultBytes, err := CmdShell(cmdString)
		// err 不为 nil 则代表没有grep 到进程，可以认为有可能被 kill -9 等操作了
		if err != nil && resultBytes != nil {
			ExitBool = false
		} else {
			log.Println(cmdString, " result:", string(resultBytes), " err:", err)
		}
		if ExitBool {
			log.Println("Birostd server quit without updating PID file ; File:", config.BifrostPidFile, "; Error:", err2)
			os.Exit(1)
		}
	}
	os.Truncate(config.BifrostPidFile, 0)
	pidFileFd.Seek(0, 0)
	io.WriteString(pidFileFd, fmt.Sprint(os.Getpid()))
}

func CmdShell(cmdString string) ([]byte, error) {
	switch runtime.GOOS {
	case "linux", "darwin", "freebsd":
		cmd := exec.Command("/bin/bash", "-c", cmdString)
		return cmd.Output()
		break
	default:
		break
	}
	return nil, fmt.Errorf(runtime.GOOS + " not supported")
}

func doSaveDbInfo() {
	if os.Getppid() != 1 && BifrostDaemon {
		return
	}
	server.DoSaveSnapshotData()
}

func doRecovery() {
	server.DoRecoverySnapshotData()
}

func doSeverDbInfoFun() {
	log.Println("save db server info data start... ")
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
			log.Println(string(debug.Stack()))
		}
	}()
	l.Lock()
	defer l.Unlock()
	if saveDbInfoStatus {
		return
	}
	server.StopAllChannel()
	doSaveDbInfo()
	saveDbInfoStatus = true
	server.Close()
	log.Println("save db server info data success! ")
}

func ListenSignal() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGINT)
	for sig := range signals {
		if sig == nil {
			continue
		}
		doSeverDbInfoFun()
		if config.BifrostPidFile != "" {
			os.Remove(config.BifrostPidFile)
		}
		os.Exit(0)
	}
}
