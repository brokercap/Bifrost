package server_test

import (
	"github.com/brokercap/Bifrost/server"
	"testing"
)

func TestDb_TransferLikeTableReq(t *testing.T) {
	db := server.NewDbByNull()

	type result struct {
		src  string
		dest string
	}

	var data []result
	data = make([]result, 0)
	data = append(data, result{src: "", dest: ""})
	data = append(data, result{src: "*", dest: "*"})
	data = append(data, result{src: "*_mytest_", dest: "(.*)_mytest_$"})
	data = append(data, result{src: "mysql_*", dest: "^mysql_(.*)"})
	data = append(data, result{src: "mysql_(.*)_test", dest: "^mysql_(.*)_test$"})

	for _, v := range data {
		tmp := db.TransferLikeTableReq(v.src)
		if tmp != v.dest {
			t.Fatal(v.src, " dest:", v.dest, " but reslut:", tmp)
		} else {
			t.Log(v.src, " dest:", v.dest, " is right")
		}
	}

}
