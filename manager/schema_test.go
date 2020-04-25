package manager

import "testing"

func TestGetGrantsFor(t *testing.T) {
	uri := "xxtest:xxtest@tcp(127.0.0.1:3306)/test"
	sql,err := GetGrantsFor(DBConnect(uri))
	if err != nil{
		t.Fatal(err)
	}
	t.Log(sql)
}


func TestCheckUserSlavePrivilege(t *testing.T) {
	uri := "xxtest:xxtest@tcp(127.0.0.1:3306)/test"
	err := CheckUserSlavePrivilege(DBConnect(uri))
	if err != nil{
		t.Fatal(err)
	}
	t.Log("test success")
}
