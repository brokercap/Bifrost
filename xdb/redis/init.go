package redis

import "github.com/brokercap/Bifrost/xdb/driver"

func init() {
	driver.Register("redis", &MyConn{}, VERSION)
}
