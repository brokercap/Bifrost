package leveldb

import "github.com/brokercap/Bifrost/xdb/driver"

func init() {
	driver.Register("leveldb", &MyConn{}, VERSION)
}
