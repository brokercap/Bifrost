package mysql

import "testing"

func TestCheckMariaDBGtid(t *testing.T) {
	gtid := "0-1000-4,1-1001-1000"
	err := CheckGtid(gtid)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("TestCheckMariaDBGtid success")
}
