package xdb_test

import (
	"github.com/brokercap/Bifrost/xdb"
	"testing"
)

func setKeyVal(table, key1 string, value interface{}) error {
	client, err := xdb.GetClient("leveldb")
	if err != nil {
		return err
	}
	defer xdb.BackCient("leveldb", client)
	client.SetPrefix("xdbtest").PutKeyVal(table, key1, value)
	return nil
}

func getKeyVal(table, key1 string) ([]byte, error) {
	client, err := xdb.GetClient("leveldb")
	if err != nil {
		return nil, err
	}
	defer xdb.BackCient("leveldb", client)
	c, err := client.GetKeyValBytes(table, key1)
	return c, err
}

func TestPool(t *testing.T) {
	xdb.InitClientPool("leveldb", "./myleveldir4", 1)

	type DataSource struct {
		Name string
		Uri  string
	}

	var table, key1, key2 string
	var val1, val2 DataSource

	table = "data_source"

	key1 = "tstst1"
	val1 = DataSource{Name: "sss", Uri: "URI1"}
	err0 := setKeyVal(table, key1, val1)
	if err0 != nil {
		t.Fatal(key1, " put error:", err0)
	} else {
		t.Log(key1, " put success")
	}

	key2 = "tstst2"
	val2 = DataSource{Name: "sss22", Uri: "URI1222"}
	err0 = setKeyVal(table, key2, val2)

	if err0 != nil {
		t.Fatal(key2, " put error:", err0)
	} else {
		t.Log(key2, " put success")
	}

	c, err1 := getKeyVal(table, key1)
	t.Log(key1, " c:", string(c), "err1:", err1)

	c, err1 = getKeyVal(table, key2)
	t.Log(key2, " c:", string(c), "err1:", err1)

}
