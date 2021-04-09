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
package config

import (
	"bufio"
	"io"
	"log"
	"os"
	"strings"
)

var MyConf map[string]map[string]string

func init() {
	MyConf = make(map[string]map[string]string)
}

func DoLoadConf(conffile string) map[string]map[string]string {
	if conffile == "" {
		log.Println("no config file ")
		return nil
	}
	var per map[string]map[string]string
	per = make(map[string]map[string]string)
	f, err := os.Open(conffile)
	if err != nil {
		log.Println("open config file:", conffile, " err:", err)
		os.Exit(1)
	}
	buf := bufio.NewReader(f)
	stringKey := ""
	for {
		l, err := buf.ReadString('\n')
		line := strings.TrimSpace(l)
		if err != nil {
			if err != io.EOF {
				//fmt.Printf("config file:%s ; isn't exsit or file is nothing!",conffile)
				//os.Exit(1)
				//panic(err)
				break
			} else {
				break
			}
		}
		switch {
		case len(line) == 0:
		case line[0] == '[' && line[len(line)-1] == ']':
			stringKey = strings.TrimSpace(line[1 : len(line)-1])
			per[stringKey] = make(map[string]string)
		case line[0] == '#':
		default:
			i := strings.IndexAny(line, "=")
			per[stringKey][strings.TrimSpace(line[0:i])] = strings.TrimSpace(line[i+1:])
		}
	}
	MyConf = per
	return MyConf
}

func GetConf(module string) map[string]string {
	return MyConf[module]
}

func GetConfigVal(module string, key string) string {
	if _, ok := MyConf[module]; !ok {
		return ""
	}
	if _, ok := MyConf[module][key]; !ok {
		return ""
	}
	return MyConf[module][key]
}

func SetConfigVal(module string, key string, val string) {
	if _, ok := MyConf[module]; !ok {
		MyConf[module] = make(map[string]string, 0)
	}
	MyConf[module][key] = val
}

func DelConfig(module string, key string) {
	if _, ok := MyConf[module]; !ok {
		return
	}
	delete(MyConf[module], key)
}
