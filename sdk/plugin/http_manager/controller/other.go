package controller

import (
	"github.com/brokercap/Bifrost/admin/xgo"
	"log"
	"path"
	"strings"
)

type OtherController struct {
	xgo.Controller
}

func (c *OtherController) NotSupported() {
	result := ResultDataStruct{Status: 0, Msg: "plugin dev,not supported", Data: nil}
	c.SetJsonData(result)
	c.StopServeJSON()
}

func (c *OtherController) StaticHtml() {
	c.SetOutputByUser()
	var route string
	i := strings.IndexAny(c.Ctx.Request.RequestURI, "?")
	if i > 0 {
		route = strings.TrimSpace(c.Ctx.Request.RequestURI[0:i])
	} else {
		route = c.Ctx.Request.RequestURI
	}

	var filenameWithSuffix string
	filenameWithSuffix = path.Base(route) //获取文件名带后缀
	var fileSuffix string
	fileSuffix = path.Ext(filenameWithSuffix) //获取文件后缀
	log.Println("fileSuffix:", fileSuffix)
	switch fileSuffix {
	case ".js":
		c.Ctx.ResponseWriter.Header().Set("Content-Type", "application/javascript; charset=UTF-8")
		break
	case ".css":
		c.Ctx.ResponseWriter.Header().Set("Content-Type", "text/css; charset=UTF-8")
		break
	default:
		break
	}
	c.Ctx.ResponseWriter.Write([]byte(StaticMap[route]))
}
