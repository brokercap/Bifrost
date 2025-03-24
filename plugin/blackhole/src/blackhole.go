package src

import (
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
)

const VERSION = "v1.6.0"
const BIFROST_VERION = "v1.6.0"

func init() {
	pluginDriver.Register("blackhole", NewConn, VERSION, BIFROST_VERION)
}

func NewConn() pluginDriver.Driver {
	return &Conn{}
}

type Conn struct {
	pluginDriver.PluginDriverInterface
}
