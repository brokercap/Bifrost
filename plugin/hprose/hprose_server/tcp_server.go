package main

import (
	"github.com/brokercap/Bifrost/plugin/hprose/hprose_server/serverdo"
	"github.com/hprose/hprose-golang/rpc"
)

func main() {
	service := rpc.NewTCPServer("tcp4://0.0.0.0:4321/")
	service.Debug = true
	service.AddFunction("Insert", serverdo.Insert)
	service.AddFunction("Update", serverdo.Update)
	service.AddFunction("Delete", serverdo.Delete)
	service.AddFunction("Query", serverdo.Query)
	service.AddFunction("Commit", serverdo.Commit)
	service.AddFunction("Check", serverdo.Check)
	service.Start()
}
