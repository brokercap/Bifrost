package src

import (
	"testing"
)

func TestGetUriParam(t *testing.T) {
	url := "pwd@123@tcp(127.0.0.1:6379,127.0.0.1:6380)/16"
	pwd, network, uri, database := GetUriParam(url)

	t.Log("pwd:", pwd)
	t.Log("network:", network)
	t.Log("uri:", uri)
	t.Log("database:", database)

	url = "127.0.0.1:6379"
	pwd, network, uri, database = GetUriParam(url)

	t.Log("pwd:", pwd)
	t.Log("network:", network)
	t.Log("uri:", uri)
	t.Log("database:", database)
}
