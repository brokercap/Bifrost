package main

import "github.com/brokercap/xgo"

import "github.com/brokercap/xgo/example/manager"

func init()  {
	xgo.AddRoute("/test",&manager.TestController{},"*:Post")
}

func main()  {
	xgo.Start("0.0.0.0:9612")
}