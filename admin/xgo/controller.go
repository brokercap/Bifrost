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
	"encoding/json"
	"errors"
	"html/template"
	"log"
	"strings"
)

func init() {

}

var (
	ErrAbort = errors.New("user stop run")
)

type OutputFormat int8

const (
	JSON_TYPE  OutputFormat = 1
	JSONP_TYPE OutputFormat = 2
	HTML_TYPE  OutputFormat = 0
	OTHER_TYPE OutputFormat = -1
)

type Controller struct {
	Ctx            *Context
	ControllerName string
	ActionName     string
	Data           map[string]interface{}
	Format         OutputFormat
	Template       *template.Template
	tplArr         []string // 模板路径
}

// ControllerInterface is an interface to uniform all controller handler.
type ControllerInterface interface {
	Init(ct *Context, controllerName, actionName string)
	Prepare()
	Finish()
	StopServeJSON()
	StopServeJSONP(jsonp ...string)
	StopRun()
	NormalStop()
	IsHtmlOutput() bool
	SetOutputByUser()
	AddTemplate(tpl ...string)
	SetTemplate(t *template.Template, err error)
}

func (c *Controller) Init(ctx *Context, controllerName, actionName string) {
	ctx.Request.ParseForm()
	c.Ctx = ctx
	c.Data = make(map[string]interface{}, 0)
	c.ControllerName = controllerName
	c.ActionName = actionName
}

func (c *Controller) Prepare() {

}

func (c *Controller) Finish() {

}

func (c *Controller) SetOutputByUser() {
	c.Format = OTHER_TYPE
}

func (c *Controller) AddTemplate(tpl ...string) {
	c.tplArr = append(c.tplArr, tpl...)
}

func (c *Controller) SetTemplate(t *template.Template, err error) {
	if err != nil {
		panic(err.Error())
	}
	c.Template = t
}

func (c *Controller) SetData(key string, data interface{}) {
	c.Data[key] = data
}

func (c *Controller) SetJsonData(data interface{}) {
	c.Data["json"] = data
}

func (c *Controller) StopServeJSON() {
	c.Format = JSON_TYPE
	c.StopRun()
	panic(ErrAbort)
}

func (c *Controller) StopServeJSONP(jsonp ...string) {
	c.Format = JSONP_TYPE
	c.StopRun()
	panic(ErrAbort)
}

func (c *Controller) StopRun() {
	c.NormalStop()
	panic(ErrAbort)
}

func (c *Controller) NormalStop() {
	c.Finish()
	if c.Format == HTML_TYPE {
		switch strings.ToLower(c.Ctx.Request.Form.Get("format")) {
		case "json":
			c.Format = JSON_TYPE
			break
		case "jsonp":
			c.Format = JSONP_TYPE
			break
		default:
			if c.Ctx.Request.Header.Get("X-Requested-With") == "XMLHttpRequest" {
				c.Format = JSON_TYPE
				break
			}
			if c.Template == nil && len(c.tplArr) == 0 {
				c.Format = JSON_TYPE
			}
			break
		}
	}
	c.Ctx.ResponseWriter.WriteHeader(200)
	switch c.Format {
	case JSON_TYPE:
		var body []byte
		if _, ok := c.Data["json"]; ok {
			body, _ = json.Marshal(c.Data["json"])
		} else {
			body, _ = json.Marshal(c.Data)
		}
		c.Ctx.ResponseWriter.Write(body)
		break
	case JSONP_TYPE:
		var body []byte
		if _, ok := c.Data["json"]; ok {
			body, _ = json.Marshal(c.Data["json"])
		} else {
			body, _ = json.Marshal(c.Data)
		}
		c.Ctx.ResponseWriter.Write([]byte(body))
		break
	case OTHER_TYPE:
		break
	default:
		if c.Template == nil {
			var err error
			c.Template, err = template.ParseFiles(c.tplArr...)
			if err != nil {
				log.Println("err:", err)
				panic(err.Error())
			}
		}
		err := c.Template.Execute(c.Ctx.ResponseWriter, c.Data)
		if err != nil {
			panic(err)
		}
		break
	}
}

func (c *Controller) IsHtmlOutput() bool {
	if c.Format == HTML_TYPE {
		switch strings.ToLower(c.Ctx.Request.Form.Get("format")) {
		case "json":
			c.Format = JSON_TYPE
			break
		case "jsonp":
			c.Format = JSONP_TYPE
			break
		default:
			if c.Ctx.Request.Header.Get("X-Requested-With") == "XMLHttpRequest" {
				c.Format = JSON_TYPE
				break
			}
			break
		}
	}
	if c.Format == HTML_TYPE {
		return true
	}

	return false
}
