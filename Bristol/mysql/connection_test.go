//go:build integration
// +build integration

package mysql

import (
	"database/sql/driver"
	"log"
	"testing"
	"time"
)

var mysql_uri = "root:root@tcp(bifrost_mysql_test:3306)/mysql?charset=utf8"

func TestMyconn_Exec_Integration(t *testing.T) {
	conn := NewConnect(mysql_uri)
	log.Println("Connect over")
	//conn.Close()
	//return
	connectionId, err := testGetConnectId(conn)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("connectionId:", connectionId)

	createSQL := `CREATE TABLE IF NOT EXISTS bifrost_test.test_1 (
		id int(11) unsigned NOT NULL AUTO_INCREMENT,
		testtinyint tinyint(4) NOT NULL DEFAULT '-1',
		testsmallint smallint(6) NOT NULL DEFAULT '-2',
		testmediumint mediumint(8) NOT NULL DEFAULT '-3',
		testint int(11) NOT NULL DEFAULT '-4',
		testbigint bigint(20) NOT NULL DEFAULT '-5',
		testvarchar varchar(10) NOT NULL,
		testchar char(2) NOT NULL,
		testenum enum('en1','en2','en3') NOT NULL DEFAULT 'en1',
		testset set('set1','set2','set3') NOT NULL DEFAULT 'set1',
		testtime time NOT NULL DEFAULT '00:00:00',
		testdate date NOT NULL DEFAULT '0000-00-00',
		testyear year(4) NOT NULL DEFAULT '1989',
		testtimestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP(),
		testdatetime datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
		testfloat float(9,2) NOT NULL DEFAULT '0.00',
		testdouble double(9,2) NOT NULL DEFAULT '0.00',
		testdecimal decimal(9,2) NOT NULL DEFAULT '0.00',
		testdatatime_null datetime DEFAULT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8 PARTITION BY HASH (id) PARTITIONS 3`
	_, err = conn.Exec(createSQL, []driver.Value{})
	if err != nil {
		t.Fatal(err)
	}
	insertSQL := `INSERT INTO bifrost_test.test_1 
			(testtinyint,testsmallint,testmediumint,testint,testbigint,testvarchar,testchar,testenum,testset,testtime,testdate,testyear,testtimestamp,testdatetime,testfloat,testdouble,testdecimal,testdatatime_null)
		values
			(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
`
	args := make([]driver.Value, 18)
	args[0] = int8(8)
	args[1] = int16(16)
	args[2] = int32(24)
	args[3] = int32(32)
	args[4] = int64(64)
	args[5] = "va"
	args[6] = "c"
	args[7] = "en3"
	args[8] = "set1,set3"
	args[9] = "10:11:00"
	args[10] = "2023-09-17"
	args[11] = "2023"
	args[12] = time.Now()
	args[13] = time.Now()
	args[14] = float32(9.32)
	args[15] = float64(666.3264)
	args[16] = "9.999"
	args[17] = nil

	r, err := conn.Exec(insertSQL, args)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(r.LastInsertId())
	t.Log(r.RowsAffected())
}

func TestMyconn_query_Integration(t *testing.T) {
	conn := NewConnect(mysql_uri)
	log.Println("Connect over")
	//conn.Close()
	//return
	connectionId, err := testGetConnectId(conn)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("connectionId:", connectionId)

	selectSQL := "SELECT * FROM bifrost_test.test_1 WHERE id in (?)"
	AutoIncrementValue := make([]string, 1)
	AutoIncrementValue[0] = "1"
	args := make([]driver.Value, 0)
	args = append(args, AutoIncrementValue)
	//args = append(args, strings.Replace(strings.Trim(fmt.Sprint(AutoIncrementValue), "[]"), " ", "','", -1))
	rows, err := conn.Query(selectSQL, args)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]map[string]driver.Value, 0)
	for {
		m := make(map[string]driver.Value, len(rows.Columns()))
		dest := make([]driver.Value, len(rows.Columns()), len(rows.Columns()))
		err := rows.Next(dest)
		if err != nil {
			break
		}
		for i, fieldName := range rows.Columns() {
			m[fieldName] = dest[i]
		}
		data = append(data, m)
	}
	t.Log(data)
	t.Log("id:", data[0]["id"])
}
