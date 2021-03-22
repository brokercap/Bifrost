package manager

import (
	"github.com/brokercap/Bifrost/admin/xgo"
	"log"
)

type TestController struct {
	xgo.Controller
}

func (c *TestController) Prepare()  {
	log.Println("Prepare ..")
}

func (c *TestController) Finish()  {
	log.Println("Finish ..")
}

func (c *TestController) Post()  {
	log.Println("Post ..")
	c.Data["data"] = "success"
}