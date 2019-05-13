package main

import (
	"github.com/hprose/hprose-golang/rpc"
	"github.com/brokercap/Bifrost/hprose_server/serverdo"
)

func main() {
	service := rpc.NewTCPServer("tcp4://0.0.0.0:4321/")
	service.Debug = true
	service.AddFunction("Insert", serverdo.Insert)
	service.AddFunction("Update", serverdo.Update)
	service.AddFunction("Delete", serverdo.Delete)
	service.AddFunction("Query", serverdo.Query)
	service.AddFunction("Check", serverdo.Check)
	service.Start()
}
