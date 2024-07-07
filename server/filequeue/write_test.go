//go:build integration
// +build integration

package filequeue

import (
	"os"
	"testing"
)

func TestWrite(t *testing.T) {
	path := "E:/filequeueTest"
	q := NewQueue(path)
	c := ""
	for i := 0; i < 50000; i++ {
		c += "c"
	}
	q.Append(c)
}

func TestWrite2(t *testing.T) {
	path := "./filequeueTest/test.log"
	fd, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0700)
	if err != nil {
		t.Fatal(err)
	}
	fd.Write([]byte("myname is test!"))
	fd.WriteString("yes ,you ara right!")
	fd.Sync()
	fd.Close()
	t.Log("over")

}
