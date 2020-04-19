package filequeue

import (
	"testing"
	"os"
)

func TestWrite(t *testing.T)  {
	path := "./filequeueTest"
	q := NewQueue(path)
	q.Append("mysqlcontent1")
	q.Append("mysqlcontent2")
	q.Append("mysqlcontent33333")
}


func TestWrite2(t *testing.T)  {
	path := "./filequeueTest/test.log"
	fd,err := os.OpenFile(path,os.O_RDWR|os.O_CREATE|os.O_APPEND,0700)
	if err != nil{
		t.Fatal(err)
	}
	fd.Write([]byte("myname is test!"))
	fd.WriteString("yes ,you ara right!")
	fd.Sync()
	fd.Close()
	t.Log("over")

}
