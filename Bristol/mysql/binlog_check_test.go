package mysql

import "testing"

func TestCheckBinlogIsRight(t *testing.T)  {
	uri:="root:root@tcp(127.0.0.1:3306)/test"
	filename:="mysql-bin.000068"
	position:=uint32(4)

	err := CheckBinlogIsRight(uri,filename,position)
	if err != nil{
		t.Fatal(err)
	}

	t.Log("test success")
}


func TestGetNearestRightBinlog(t *testing.T)  {
	uri:="root:root@tcp(127.0.0.1:3306)/test"
	filename:="mysql-bin.000068"
	position:=uint32(484)

	ReplicateDoDb := make(map[string]uint8,0)
	ReplicateDoDb["test"] = 1

	newPosition := GetNearestRightBinlog(uri,filename,position,101,ReplicateDoDb)

	if newPosition == 0{
		t.Fatal("error newPosition == 0")
	}
	t.Log("test success,newPosition==",newPosition)
}
