//go:build integration
// +build integration

package filequeue

import (
	"testing"
)

func TestRead(t *testing.T) {
	path := "./filequeueTest"
	q := NewQueue(path)
	for i := 0; i <= 2; i++ {
		c, e := q.Pop()
		if e != nil {
			t.Fatal(e)
		}
		t.Log("read i:", i, " val:", c)
	}

	t.Log("over")

}

func TestReadLast(t *testing.T) {
	path := "E:/filequeueTest"
	q := NewQueue(path)
	c, err := q.ReadLast()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("c:", string(c))
	t.Log("over")
}

func TestSlice(t *testing.T) {
	List := make([]int, 0)
	List = append(List, 0)
	List = append(List, 1)
	List = append(List, 2)
	List = append(List, 3)
	List = append(List, 4)
	List = append(List, 5)
	t.Log(List[0:1])
}
