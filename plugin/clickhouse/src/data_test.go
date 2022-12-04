package src_test

import (
	"context"
	"strings"
	"testing"
	"time"
)

import (
	"database/sql/driver"
	MyPlugin "github.com/brokercap/Bifrost/plugin/clickhouse/src"
	"github.com/shopspring/decimal"
)

func TestClickhouseDB_Exec(t *testing.T) {
	SchemaName = "bifrost_test"
	TableName = "write_test"
	c := MyPlugin.NewClickHouseDBConn(url)
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

	ctx := context.Background()
	stmt, err := c.GetConn().PrepareBatch(ctx, sqlInsert)
	if err != nil {
		if !strings.Contains(err.Error(), "Decimal128 is not supported") {
			t.Fatal(err)
		} else {
			t.Log(err)
			return
		}
	}
	val := make([]interface{}, 0)
	val = append(val, int64(1))
	val = append(val, decimal.New(0, 199))
	val = append(val, decimal.New(123131230, 19))

	err = stmt.Append(val...)
	if err != nil {
		t.Fatal(err)
	}
	err = stmt.Send()
	if err != nil {
		t.Fatal(err)
	}
	err = c.GetConn().Close()
	if err != nil {
		t.Fatal(err)
	}

}

func TestClickhouseDB_DateTime64_Exec(t *testing.T) {
	SchemaName = "bifrost_test"
	TableName = "write_datetime_test"
	c := MyPlugin.NewClickHouseDBConn(url)
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
	ctx := context.Background()
	stmt, err := c.GetConn().PrepareBatch(ctx, sqlInsert)
	if err != nil {
		t.Fatal(err)
	}
	val := make([]interface{}, 0)
	val = append(val, int64(1))
	val = append(val, time.Now().Truncate(time.Second))
	val = append(val, "2006-01-02 15:04:05.123")

	err = stmt.Append(val...)
	if err != nil {
		t.Fatal(err)
	}
	err = stmt.Send()
	if err != nil {
		t.Fatal(err)
	}
	err = c.GetConn().Close()
	if err != nil {
		t.Fatal(err)
	}

}

func TestClickhouseDB_Conn(t *testing.T) {
	c := MyPlugin.NewClickHouseDBConn(url)
	c.Close()
	t.Log("success")
}
