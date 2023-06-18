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

package prometheus

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type Config struct {
	Url              string `json:"url"`
	TimeInterval     int    `json:"input.time.interval,string"`
	Start            int    `json:"start,string"`
	End              int    `json:"end,string"`
	HttpTimeoutParam int    `json:"input.http.timeout,string"`

	HttpTimeout        time.Duration `json:"-"`
	lastSuccessEndTime int
	lastTmpEndTime     int
}

func ParseDSN(dsn string) (params map[string]string) {
	params = make(map[string]string, 0)
	if dsn == "" {
		return
	}
	pArr := strings.Split(dsn, "?")
	if len(pArr) == 1 {
		params["url"] = pArr[0]
		return
	}
	paramStr := pArr[1]
	for _, v := range strings.Split(paramStr, "&") {
		param := strings.SplitN(v, "=", 2)
		if len(param) != 2 {
			continue
		}
		params[param[0]] = param[1]
	}
	params["url"] = fmt.Sprintf("%s?query=%s", pArr[0], params["query"])
	return params
}

func getConfig(params map[string]string) (*Config, error) {
	c, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	var data Config
	err = json.Unmarshal(c, &data)
	if data.TimeInterval == 0 {
		data.TimeInterval = 300
	}
	if data.HttpTimeoutParam <= 0 {
		data.HttpTimeout = 30 * time.Second
	} else {
		data.HttpTimeout = time.Duration(data.HttpTimeoutParam) * time.Millisecond
	}

	return &data, err
}
