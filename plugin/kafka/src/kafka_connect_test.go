package src

import "testing"

func TestKafkaConnect(t *testing.T) {
	myConn := MyConn{}
	err := myConn.CheckUri("192.168.137.40:9092")
	if err != nil {
		println(err.Error())
	}
}
