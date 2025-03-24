//go:build integration
// +build integration

package mysql

import "testing"

func TestCheckBinlogIsRight_Integration(t *testing.T) {
	filename := "mysql-bin.000068"
	position := uint32(4)

	err := CheckBinlogIsRight(mysql_uri, filename, position)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("test success")
}

func TestGetNearestRightBinlog_Integration(t *testing.T) {
	filename := "mysql-bin.000068"
	position := uint32(484)

	ReplicateDoDb := make(map[string]map[string]uint8, 0)

	ReplicateDoDb["bifrost_test"] = make(map[string]uint8, 0)
	ReplicateDoDb["bifrost_test"]["binlog_field_test"] = 1
	newPosition := GetNearestRightBinlog(mysql_uri, filename, position, 101, ReplicateDoDb, nil)

	if newPosition == 0 {
		t.Fatal("error newPosition == 0")
	}
	t.Log("test success,newPosition==", newPosition)
}
