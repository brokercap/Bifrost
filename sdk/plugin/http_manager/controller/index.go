package controller

import (
	"github.com/brokercap/Bifrost/admin/xgo"
	"net/http"
)

type IndexController struct {
	xgo.Controller
}

func (c *IndexController) Index() {
	c.SetOutputByUser()
	http.Redirect(c.Ctx.ResponseWriter, c.Ctx.Request, "/db/detail?DbName=mysqlTest", http.StatusFound)
}
