//go:build integration
// +build integration

package mysql

import (
	"log"
	"strings"
	"testing"
)
import (
	"database/sql/driver"
	"fmt"
)

func testGetConnectId(conn MysqlConnection) (connectionId string, err error) {
	//*** get connection id start
	sql := "SELECT connection_id()"
	var stmt driver.Stmt
	log.Println("Prepare start")
	stmt, err = conn.Prepare(sql)
	log.Println("Prepare over")
	if err != nil {
		return
	}
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	if err != nil {
		stmt.Close()
		return
	}
	for {
		dest := make([]driver.Value, 1, 1)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		connectionId = fmt.Sprint(dest[0])
		break
	}
	rows.Close()
	stmt.Close()
	return
}

func TestMysqlDriver_for_CachingSha2Password_Open_Integration(t *testing.T) {
	conn := NewConnect(mysql_uri)
	log.Println("Connect over")
	//conn.Close()
	//return
	connectionId, err := testGetConnectId(conn)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("connectionId:", connectionId)
}

func TestMysqlDriver_for_NativePassword_Open_Integration(t *testing.T) {
	conn := NewConnect(mysql_uri)
	connectionId, err := testGetConnectId(conn)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("connectionId:", connectionId)
}

func TestMysqlDriver_for_CachingSha2Password_switchTo_NativePassword_Open_Integration(t *testing.T) {
	conn := NewConnect(mysql_uri)
	connectionId, err := testGetConnectId(conn)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("connectionId:", connectionId)
}

func TestMysqlDriver_for_NativePassword_Open_2_use_database_Integration(t *testing.T) {
	conn := NewConnect(mysql_uri)
	connectionId, err := testGetConnectId(conn)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("connectionId:", connectionId)

	conn.(driver.Execer).Exec("USE bifrost_test", []driver.Value{})

	sql := "show tables"
	stmt, err := conn.Prepare(sql)
	if err != nil {
		return
	}
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	if err != nil {
		stmt.Close()
		return
	}
	var tableArr = make([]string, 0)
	for {
		dest := make([]driver.Value, 1, 1)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		tableName := dest[0].(string)
		tableArr = append(tableArr, tableName)
		break
	}
	rows.Close()
	stmt.Close()

	conn.Close()

	t.Log(tableArr)
	return

}

func TestMysqlDriver_for_charset_Integration(t *testing.T) {
	uri := strings.ReplaceAll(mysql_uri, "charset=utf8", "charset=gbk")
	conn := NewConnect(uri)
	connectionId, err := testGetConnectId(conn)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("connectionId:", connectionId)

	sql := "show tables"
	stmt, err := conn.Prepare(sql)
	if err != nil {
		return
	}
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	if err != nil {
		stmt.Close()
		return
	}
	var tableArr = make([]string, 0)
	for {
		dest := make([]driver.Value, 1, 1)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		tableName := dest[0].(string)
		tableArr = append(tableArr, tableName)
		break
	}
	rows.Close()
	stmt.Close()

	t.Log("tableArr:", tableArr)

	sql = "select * from bifrost_test.c_test"
	stmt, err = conn.Prepare(sql)
	if err != nil {
		t.Fatal(err)
		return
	}
	p = make([]driver.Value, 0)
	rows, err = stmt.Query(p)
	if err != nil {
		t.Fatal(err)
		stmt.Close()
		return
	}
	var dataArr = make([]map[string]interface{}, 0)
	for {
		dest := make([]driver.Value, 3, 3)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		m := make(map[string]interface{}, 0)
		m["id"] = dest[0]
		m["c_1"] = dest[1]
		m["c_2"] = dest[2]

		dataArr = append(dataArr, m)
		continue
	}
	rows.Close()
	stmt.Close()

	conn.Close()

	t.Log("dataArr:", dataArr)
	return

}

func TestMysqlDriver_for_56_Open_Integration(t *testing.T) {
	conn := NewConnect(mysql_uri)
	connectionId, err := testGetConnectId(conn)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("connectionId:", connectionId)
}
