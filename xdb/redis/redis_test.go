//go:build integration
// +build integration

package redis_test

import (
	"github.com/brokercap/Bifrost/xdb/driver"
	"github.com/brokercap/Bifrost/xdb/redis"
	"testing"
)

func getConn() (driver.XdbDriver, error) {
	uri := "192.168.220.130:6379"
	MyConn := redis.MyConn{}
	return MyConn.Open(uri)
}

func TestConn_GetKeyVal(t *testing.T) {
	conn, err := getConn()
	if err != nil {
		t.Fatal(err)
	}
	key1 := "key1"
	r, err := conn.GetKeyVal([]byte(key1))
	if err != nil {
		t.Fatal(err)
	}

	t.Log("result:", string(r))
}

func TestConn_PutKeyVal(t *testing.T) {
	conn, err := getConn()
	if err != nil {
		t.Fatal(err)
	}
	key1 := "key1"
	val1 := "val1"
	err = conn.PutKeyVal([]byte(key1), []byte(val1))
	if err != nil {
		t.Fatal(err)
	}

	r, err := conn.GetKeyVal([]byte(key1))
	if err != nil {
		t.Fatal(err)
	}

	if string(r) != val1 {
		t.Fatal("result:", string(r), " != ", val1)
	}
	t.Log("result:", string(r))
}

func TestConn_DelKeyVal(t *testing.T) {
	conn, err := getConn()
	if err != nil {
		t.Fatal(err)
	}
	key1 := "key2"

	err = conn.DelKeyVal([]byte(key1))
	if err != nil {
		t.Fatal(err)
	}

	t.Log("del test success")
}

func TestConn_GetListByKeyPrefix(t *testing.T) {
	conn, err := getConn()
	if err != nil {
		t.Fatal(err)
	}
	key1 := "key"

	dataList, err := conn.GetListByKeyPrefix([]byte(key1))
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range dataList {
		t.Log(v.Key, " = ", v.Value)
	}
	t.Log("GetListByKeyPrefix test success")
}
