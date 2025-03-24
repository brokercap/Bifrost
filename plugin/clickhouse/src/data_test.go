//go:build integration
// +build integration

package src

import (
	"database/sql/driver"
	"strings"
	"testing"
)

func TestClickhouseDB_Exec_Integration(t *testing.T) {
	SchemaName = "bifrost_test"
	TableName = "write_test"
	c := NewClickHouseDBConn(url)
	sql1 := "CREATE DATABASE IF NOT EXISTS  `" + SchemaName + "`"
	var err error
	err = c.Exec(sql1, []driver.Value{})
	if err != nil {
		t.Fatal(err)
	}
	sql2 := "CREATE TABLE IF NOT EXISTS " + SchemaName + "." + TableName + "(id Int64,Decimal64Test Decimal(10,4),Decimal128Test Decimal(38,5)) ENGINE = " + engine + " ORDER BY (id);"
	err = c.Exec(sql2, []driver.Value{})
	if err != nil {
		t.Fatal(err)
	}

	sqlInsert := "INSERT INTO " + SchemaName + "." + TableName + " (id,Decimal64Test,Decimal128Test) VALUES (?,?,?)"
	_, err = c.GetConn().Begin()
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := c.GetConn().Prepare(sqlInsert)
	if err != nil {
		if !strings.Contains(err.Error(), "Decimal128 is not supported") {
			t.Fatal(err)
		} else {
			t.Log(err)
			return
		}
	}
	val := make([]driver.Value, 0)
	val = append(val, int64(1))
	val = append(val, float64(0.199))
	val = append(val, "9221511215120215152121.5225")

	Result, err := stmt.Exec(val)
	if err != nil {
		t.Fatal(err)
	}
	err = c.GetConn().Commit()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(Result)
}

func TestClickhouseDB_DateTime64_Exec_Integration(t *testing.T) {
	SchemaName = "bifrost_test"
	TableName = "write_datetime_test"
	c := NewClickHouseDBConn(url)
	sql1 := "CREATE DATABASE IF NOT EXISTS  `" + SchemaName + "`"
	var err error
	err = c.Exec(sql1, []driver.Value{})
	if err != nil {
		t.Fatal(err)
	}
	sql2 := "CREATE TABLE IF NOT EXISTS " + SchemaName + "." + TableName + "(id Int64,datetimeTest1 Datetime,datetime64Test1 Datetime64) ENGINE = " + engine + " ORDER BY (id);"
	err = c.Exec(sql2, []driver.Value{})
	if err != nil {
		t.Fatal(err)
	}

	sqlInsert := "INSERT INTO " + SchemaName + "." + TableName + " (id,datetimeTest1,datetime64Test1) VALUES (?,?,?)"
	_, err = c.GetConn().Begin()
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := c.GetConn().Prepare(sqlInsert)
	if err != nil {
		t.Fatal(err)
	}
	val := make([]driver.Value, 0)
	val = append(val, int64(1))
	val = append(val, int64(0))
	val = append(val, "2006-01-02 15:04:05.123")

	Result, err := stmt.Exec(val)
	if err != nil {
		t.Fatal(err)
	}
	err = c.GetConn().Commit()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(Result)
}

func TestClickhouseDB_Conn_Integration(t *testing.T) {
	c := NewClickHouseDBConn(url)
	c.GetConn().Begin()
	err := c.GetConn().Rollback()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("success")
}
