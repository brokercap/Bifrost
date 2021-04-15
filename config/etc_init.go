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
/*
初始化配置
*/
package config

import (
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
)

func LoadConf(BifrostConfigFile string) {
	if BifrostConfigFile == "" {
		BifrostConfigFile = BifrostDir + "/etc/Bifrost.ini"
		//log.Println("BifrostConfigFile:",BifrostConfigFile)
	} else {
		if runtime.GOOS != "windows" {
			if BifrostConfigFile[0:1] != "/" {
				BifrostConfigFile = BifrostDir + BifrostConfigFile
			}
		}
	}
	DoLoadConf(BifrostConfigFile)
}

func InitParam() {

	// 监听端口
	Listen = GetConfigVal("Bifrostd", "listen")
	if Listen == "" {
		Listen = "0.0.0.0:21036"
	}
	//从 map中清除，为了省内存
	DelConfig("Bifrostd", "listen")

	// 数据存储目录
	DataDir = GetConfigVal("Bifrostd", "data_dir")

	if DataDir == "" {
		DataDir = BifrostDir + "/data"
	}

	if runtime.GOOS != "windows" {
		if DataDir[0:1] != "/" {
			DataDir = BifrostDir + DataDir
		}
	}
	DelConfig("Bifrostd", "data_dir")

	if runtime.GOOS != "windows" {
		BifrostPidFile = GetConfigVal("Bifrostd", "pid")
		if BifrostPidFile == "" {
			BifrostPidFile = DataDir + "/Bifrost.pid"
		}
	}
	DelConfig("Bifrostd", "pid")

	BifrostLogDir = GetConfigVal("Bifrostd", "log_dir")
	if BifrostLogDir == "" {
		log.Println("no config [ Bifrostd log_dir ] ")
		BifrostLogDir = BifrostDir + "/logs"
		log.Println("log_dir default:", BifrostLogDir)
	}
	if runtime.GOOS != "windows" {
		if BifrostLogDir[0:1] != "/" {
			BifrostLogDir = BifrostDir + BifrostLogDir
		}
	}
	DelConfig("Bifrostd", "log_dir")

	BifrostAdminTemplateDir = GetConfigVal("Bifrostd", "admin_template_dir")
	if BifrostAdminTemplateDir == "" {
		BifrostAdminTemplateDir = BifrostDir + "/admin/view"
	}
	if runtime.GOOS != "windows" {
		if BifrostAdminTemplateDir[0:1] != "/" {
			BifrostAdminTemplateDir = BifrostDir + BifrostAdminTemplateDir
		}
	}
	DelConfig("Bifrostd", "admin_template_dir")

	BifrostPluginTemplateDir = GetConfigVal("Bifrostd", "plugin_template_dir")
	if BifrostPluginTemplateDir == "" {
		BifrostPluginTemplateDir = BifrostDir
	}
	if runtime.GOOS != "windows" {
		if BifrostPluginTemplateDir[0:1] != "/" {
			BifrostPluginTemplateDir = BifrostDir + BifrostPluginTemplateDir
		}
	}
	DelConfig("Bifrostd", "plugin_template_dir")

	var tmp string
	tmp = GetConfigVal("Bifrostd", "toserver_queue_size")
	if tmp != "" {
		intA, err := strconv.Atoi(tmp)
		if err == nil && intA > 0 {
			ToServerQueueSize = intA
		} else {
			log.Println("Bifrost.ini Bifrostd.toserver_queue_size type conversion to int err:", err)
		}
	}
	DelConfig("Bifrostd", "toserver_queue_size")

	if GetConfigVal("Bifrostd", "dynamic_plugin") == "true" {
		DynamicPlugin = true
		return
	}
	DelConfig("Bifrostd", "dynamic_plugin")

	tmp = GetConfigVal("Bifrostd", "channel_queue_size")
	if tmp != "" {
		intA, err := strconv.Atoi(tmp)
		if err == nil && intA > 0 {
			ChannelQueueSize = intA
		} else {
			log.Println("Bifrost.ini Bifrostd.channel_queue_size type conversion to int err:", err)
		}
	}
	DelConfig("Bifrostd", "channel_queue_size")

	tmp = GetConfigVal("Bifrostd", "count_queue_size")
	if tmp != "" {
		intA, err := strconv.Atoi(tmp)
		if err == nil && intA > 0 {
			CountQueueSize = intA
		} else {
			log.Println("Bifrost.ini Bifrostd.count_queue_size type conversion to int err:", err)
		}
	}
	DelConfig("Bifrostd", "count_queue_size")

	tmp = GetConfigVal("Bifrostd", "key_cache_pool_size")
	if tmp != "" {
		intA, err := strconv.Atoi(tmp)
		if err == nil && intA > 0 {
			KeyCachePoolSize = intA
		} else {
			log.Println("Bifrost.ini Bifrostd.key_cache_pool_size type conversion to int err:", err)
		}
	}
	DelConfig("Bifrostd", "key_cache_pool_size")

	if GetConfigVal("Bifrostd", "file_queue_usable") == "false" {
		FileQueueUsable = false
	}
	DelConfig("Bifrostd", "file_queue_usable")

	tmp = GetConfigVal("Bifrostd", "file_queue_usable_count")
	if tmp != "" {
		intA, err := strconv.ParseUint(tmp, 10, 32)
		if err == nil && intA > 0 {
			FileQueueUsableCount = uint32(intA)
		} else {
			log.Println("Bifrost.ini Bifrostd.file_queue_usable_count type conversion to uint32 err:", err)
		}
	}
	DelConfig("Bifrostd", "file_queue_usable_count")

	tmp = GetConfigVal("Bifrostd", "file_queue_usable_count_time_diff")
	if tmp != "" {
		intA, err := strconv.ParseInt(tmp, 10, 64)
		if err == nil && intA > 0 {
			FileQueueUsableCountTimeDiff = intA
		} else {
			log.Println("Bifrost.ini Bifrostd.file_queue_usable_count_time_diff type conversion to int64 err:", err)
		}
	}
	DelConfig("Bifrostd", "file_queue_usable_count_time_diff")

	tmp = GetConfigVal("Bifrostd", "plugin_commit_timeout")
	if tmp != "" {
		intA, err := strconv.Atoi(tmp)
		if err == nil && intA > 0 {
			PluginCommitTimeOut = intA
		} else {
			log.Println("Bifrost.ini Bifrostd.plugin_commit_timeout type conversion to int err:", err)
		}
	}
	DelConfig("Bifrostd", "plugin_commit_timeout")

	tmp = GetConfigVal("Bifrostd", "plugin_sync_retry_time")
	if tmp != "" {
		intA, err := strconv.Atoi(tmp)
		if err == nil && intA > 0 {
			PluginSyncRetrycTime = intA
		} else {
			log.Println("Bifrost.ini Bifrostd.plugin_sync_retry_time type conversion to int err:", err)
		}
	}
	DelConfig("Bifrostd", "plugin_sync_retry_time")

	tmp = GetConfigVal("Bifrostd", "refuse_ip_login_failed_count")
	if tmp != "" {
		intA, err := strconv.Atoi(tmp)
		if err == nil && intA >= 0 {
			RefuseIpLoginFailedCount = intA
		} else {
			log.Println("Bifrost.ini Bifrostd.refuse_ip_login_failed_count type conversion to int err:", err)
		}
	}
	DelConfig("Bifrostd", "refuse_ip_login_failed_count")

	tmp = GetConfigVal("Bifrostd", "refuse_ip_timeout")
	if tmp != "" {
		intA, err := strconv.ParseInt(tmp, 10, 64)
		if err == nil && intA >= 0 {
			RefuseIpTimeOut = intA
		} else {
			log.Println("Bifrost.ini Bifrostd.refuse_ip_timeout type conversion to int64 err:", err)
		}
	}
	DelConfig("Bifrostd", "refuse_ip_timeout")

	initTLSParam()
}

func initTLSParam() {
	var path string
	var tlsKeyFile string = GetConfigVal("Bifrostd", "tls_key_file")
	var tlsCrtFile string = GetConfigVal("Bifrostd", "tls_crt_file")

	var checkFileStat = func(pathDir, file string) error {
		_, err := os.Stat(pathDir + "/" + file)
		return err
	}

	var setTLSConfig = func() {
		TLSServerKeyFile = path + "/" + tlsKeyFile
		TLSServerCrtFile = path + "/" + tlsCrtFile
		TLS = true
	}

	if GetConfigVal("Bifrostd", "tls") == "true" {
		for {
			// 相对于 Bifrost 二进制可执行文件的路径
			path, _ = filepath.Abs(filepath.Dir(os.Args[0]))
			if checkFileStat(path, tlsKeyFile) == nil && checkFileStat(path, tlsCrtFile) == nil {
				setTLSConfig()
				break
			}
			// 相对于 Bifost 二进制可执行文件的父目录的路径
			path = filepath.Dir(path)
			if checkFileStat(path, tlsKeyFile) == nil && checkFileStat(path, tlsCrtFile) == nil {
				setTLSConfig()
				break
			}
			// 相对于 Bifrost.ini 配置文件的路径
			path, _ = filepath.Abs(filepath.Dir(BifrostConfigFile))
			BifrostConfigFilePath := path
			if checkFileStat(path, tlsKeyFile) == nil && checkFileStat(path, tlsCrtFile) == nil {
				setTLSConfig()
				break
			}
			// 绝对路径
			path = ""
			if checkFileStat(path, tlsKeyFile) == nil && checkFileStat(path, tlsCrtFile) == nil {
				setTLSConfig()
				break
			}
			log.Println("tls_key_file:", BifrostConfigFilePath+"/"+tlsKeyFile, " or tls_crt_file: ", BifrostConfigFilePath+"/"+tlsCrtFile, " not exsit")
			break
		}
	}

	DelConfig("Bifrostd", "tls_key_file")
	DelConfig("Bifrostd", "tls_crt_file")
	DelConfig("Bifrostd", "tls")
}
