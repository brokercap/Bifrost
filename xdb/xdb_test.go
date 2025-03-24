//go:build integration
// +build integration

package xdb_test

import (
	"github.com/brokercap/Bifrost/xdb"
	"log"
	"os"
	"testing"
)

func TestClient(t *testing.T) {
	client, err := xdb.NewClient("leveldb", "./myleveldir4")
	if err != nil {
		t.Fatal(err)
		os.Exit(1)
	}
	defer client.Close()
	client.SetPrefix("xdbtest")

	type DataSource struct {
		Name string
		Uri  string
	}

	var table, key1, key2 string
	var val1, val2 DataSource

	table = "data_source"

	key1 = "tstst1"
	val1 = DataSource{Name: "sss", Uri: "URI1"}
	err0 := client.PutKeyVal(table, key1, val1)
	if err0 != nil {
		t.Fatal(key1, " put error:", err0)
	} else {
		t.Log(key1, " put success")
	}

	key2 = "tstst2"
	val2 = DataSource{Name: "sss22", Uri: "URI1222"}
	err0 = client.PutKeyVal(table, key2, val2)

	if err0 != nil {
		t.Fatal(key2, " put error:", err0)
	} else {
		t.Log(key2, " put success")
	}

	var data1 DataSource
	c1, err1 := client.GetKeyVal(table, key1, &data1)
	t.Log("data1:", data1, "   c1:", string(c1))
	if err1 != nil {
		t.Fatal(err1)
	}

	var data3 []DataSource
	c2, err2 := client.GetListByKeyPrefix(table, "", &data3)
	t.Log(" c2:", c2)
	if err2 != nil {
		t.Fatal(err2)
	}

	for _, v := range c2 {
		log.Println(v.Key, "val:", v.Value)
	}

}
