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

package src

import (
	"strings"
)

func ParseDSN(dsn string) (params map[string]string) {
	params = make(map[string]string, 0)
	if dsn == "" {
		return
	}
	var index int
	var addr string
	var paramStr string
	index = strings.Index(dsn, "?")
	//  127.0.0.1:9092,192.168.1.100?param=p1&param2=p2
	// ==> 127.0.0.1:9092,192.168.1.100  param=p1   param2=p2
	if index <= 0 {
		addr = dsn
	} else {
		addr = dsn[0:index]
		paramStr = dsn[index+1:]
	}
	params["addr"] = addr
	if paramStr == "" {
		return
	}
	for _, v := range strings.Split(paramStr, "&") {
		param := strings.SplitN(v, "=", 2)
		if len(param) != 2 {
			continue
		}
		params[param[0]] = param[1]
	}
	return params
}
