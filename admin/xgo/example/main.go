package main

import (
	"github.com/brokercap/Bifrost/admin/xgo"
	"github.com/brokercap/Bifrost/admin/xgo/example/manager"
)

func init() {
	xgo.Router("/test", &manager.TestController{}, "*:Post")
}

func main() {
	xgo.Start("0.0.0.0:9612")
}
