package mysql

import (
	inputDriver "github.com/brokercap/Bifrost/input/driver"
)

const (
	VERSION         string = "v2.3.12"
	BIFROST_VERSION string = "v2.3.12"
)

func init() {
	inputDriver.Register("mysql", NewInputPlugin, VERSION, BIFROST_VERSION)
}
