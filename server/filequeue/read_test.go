package filequeue

import (
	"testing"
)

func TestRead(t *testing.T)  {
	path := "./filequeueTest"
	q := NewQueue(path)
	for i:=0;i<=2;i++{
		c,e:=q.Pop()
		if e!=nil{
			t.Fatal(e)
		}
		t.Log("read i:",i," val:",c)
	}

	t.Log("over")

}


func TestReadLast(t *testing.T)  {
	path := "./filequeueTest"
	q := NewQueue(path)
	c,err := q.ReadLast()
	if err!= nil{
		t.Fatal(err)
	}
	t.Log("c:",c)
	t.Log("over")

}