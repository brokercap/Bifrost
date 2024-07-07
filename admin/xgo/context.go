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
package xgo

import (
	"net/http"
	"strconv"
	"strings"
)

type Context struct {
	Request        *http.Request
	ResponseWriter http.ResponseWriter
	Session        *SessionMgr
}

func (ctx *Context) GetParamInt64(key string, defaultVal ...int64) (int64, error) {
	val := ctx.Request.Form.Get(key)
	int64, err := strconv.ParseInt(val, 10, 64)
	if len(defaultVal) == 0 {
		return int64, err
	}
	if err != nil {
		return defaultVal[0], err
	}
	return int64, err
}

func (ctx *Context) GetParamUInt64(key string, defaultVal ...uint64) (uint64, error) {
	val := ctx.Request.Form.Get(key)
	uint64, err := strconv.ParseUint(val, 10, 64)
	if len(defaultVal) == 0 {
		return uint64, err
	}
	if err != nil {
		return defaultVal[0], err
	}
	return uint64, err
}

func (ctx *Context) GetParamBool(key string, defaultVal ...bool) bool {
	val := ctx.Request.Form.Get(key)
	if val == "" || len(defaultVal) > 0 {
		return defaultVal[0]
	}
	if strings.ToLower(val) == "true" {
		return true
	} else {
		return false
	}
}

func (ctx *Context) Get(key string, defaultVal ...string) string {
	val := ctx.Request.Form.Get(key)
	if val == "" && len(defaultVal) > 0 {
		return defaultVal[0]
	}
	return ctx.Request.Form.Get(key)
}
