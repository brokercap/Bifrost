package plugin

import (
	"fmt"
	"github.com/brokercap/Bifrost/config"
	"github.com/brokercap/Bifrost/plugin/driver"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"plugin"
	"runtime"
	"time"
)

var lastLoadPluginTime int64 = 0
var pluginDir string = ""

var errorPluginMap map[string]driver.DriverStructure

func init() {
	errorPluginMap = make(map[string]driver.DriverStructure, 0)
}

func DoDynamicPlugin() {
	if runtime.GOOS != "linux" {
		log.Println(runtime.GOOS, "don't support dynamic plugin")
		return
	}
	if !config.DynamicPlugin {
		log.Println("don't support dynamic plugin")
		return
	}
	log.Println("load dynamic plugin every 60s")

	execPath, _ := exec.LookPath(os.Args[0])
	pluginDir = filepath.Dir(execPath) + "/plugin/"
	go func() {
		for {
			LoadPlugin()
			time.Sleep(60 * time.Second)
		}
	}()
}

func GetErrorPluginList() map[string]driver.DriverStructure {
	return errorPluginMap
}

func cleanErrorPluginMap() {
	errorPluginMap = make(map[string]driver.DriverStructure, 0)
}

func LoadPlugin() error {
	if runtime.GOOS != "linux" {
		return fmt.Errorf("system: " + runtime.GOOS + " can not support plugin; only linux can be")
	}
	dirInfo, err := os.Stat(pluginDir)
	if err != nil {
		cleanErrorPluginMap()
		return nil
	}
	if dirInfo.ModTime().Unix() < lastLoadPluginTime {
		cleanErrorPluginMap()
		return nil
	}
	dirs, _ := ioutil.ReadDir(pluginDir)
	pluginSoMap := make(map[string]string, 0)
	for _, dir := range dirs {
		if dir.ModTime().Unix() < lastLoadPluginTime {
			continue
		}
		if dir.IsDir() {
			files, _ := ioutil.ReadDir(pluginDir + dir.Name())
			//log.Println("dirs",pluginDir+dir.Name())
			for _, file := range files {
				if file.IsDir() {
					continue
				}
				fileSuffix := path.Ext(file.Name())
				//log.Println("file",pluginDir+dir.Name()+"/"+file.Name())
				if fileSuffix == ".so" {
					if file.ModTime().Unix() < lastLoadPluginTime {
						continue
					}
					pluginSoMap[dir.Name()] = pluginDir + dir.Name() + "/" + file.Name()
				}
			}
		}
	}
	lastLoadPluginTime = time.Now().Unix()
	if len(pluginSoMap) == 0 {
		cleanErrorPluginMap()
		return nil
	}
	for name, v := range pluginSoMap {
		_, err := plugin.Open(v)
		if err != nil {
			log.Println("plugin load so:", v, " err:", err)
			errorPluginMap[name] = driver.DriverStructure{
				Error: err.Error() + "; Current Bifrost plugin API_VERSION : " + driver.GetApiVersion(),
			}
			continue
		} else {
			//只要加载成功 就删除，以便 后面做对比
			delete(errorPluginMap, name)
			delete(pluginSoMap, name)
		}

		log.Println("plugin load success so:", v)
	}
	//log.Println(errorPluginMap)

	//这里要把有可能第一次so 加载失败了,然后删除了so文件，需要把错误信息给删除掉
	for name, _ := range errorPluginMap {
		if _, ok := pluginSoMap[name]; !ok {
			delete(errorPluginMap, name)
		}
	}
	return nil
}
