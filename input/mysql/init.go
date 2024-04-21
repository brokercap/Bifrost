package mysql

import (
	inputDriver "github.com/brokercap/Bifrost/input/driver"
)

const (
	VERSION         string = "v2.0.0"
	BIFROST_VERSION string = "v2.0.0"
)

func init() {
	inputDriver.Register("mysql", NewInputPlugin, VERSION, BIFROST_VERSION)
}
