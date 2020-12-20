package mysql

import "testing"

func Test_driver_connect(t *testing.T) {
	driver := mysqlDriver{}
	open, err := driver.Open("root:Root@163.@tcp(47.90.43.60:3307)/sisilily")
	println(open, err)
}
