//go:build integration
// +build integration

package sql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/brokercap/Bifrost/Bristol/mysql"
	"log"
	"testing"
)

/*
CREATE TABLE `binlog_field_test3` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `testtinyint` tinyint(4) DEFAULT NULL,
  `testsmallint` smallint(6) DEFAULT NULL,
  `testmediumint` mediumint(8) DEFAULT NULL,
  `testint` int(11) DEFAULT NULL,
  `testbigint` bigint(20) DEFAULT NULL,
  `testvarchar` varchar(10) DEFAULT NULL,
  `testchar` char(2) DEFAULT NULL,
  `testenum` enum('en1','en2','en3') DEFAULT NULL,
  `testset` set('set1','set2','set3') DEFAULT NULL,
  `testtime` time DEFAULT NULL,
  `testdate` date DEFAULT NULL,
  `testyear` year(4) DEFAULT NULL,
  `testtimestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `testdatetime` datetime DEFAULT NULL,
  `testfloat` float(9,2) DEFAULT NULL,
  `testdouble` double(9,2) DEFAULT NULL,
  `testdecimal` decimal(9,2) DEFAULT NULL,
  `testtext` text,
  `testblob` blob,
  `testbit` bit(64) DEFAULT NULL,
  `testbool` tinyint(1) DEFAULT NULL,
  `testmediumblob` mediumblob,
  `testlongblob` longblob,
  `testtinyblob` tinyblob,
  `test_unsinged_tinyint` tinyint(4) unsigned DEFAULT NULL,
  `test_unsinged_smallint` smallint(6) unsigned DEFAULT NULL,
  `test_unsinged_mediumint` mediumint(8) unsigned DEFAULT NULL,
  `test_unsinged_int` int(11) unsigned DEFAULT NULL,
  `test_unsinged_bigint` bigint(20) unsigned DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=3 DEFAULT CHARSET=utf8
*/

func TestChekcDataTypeByNull(t *testing.T) {
	SchemaName := "bifrost_test"
	TableName := "binlog_field_test"
	Uri := "root:root@tcp(10.40.2.41:3306)/test"
	db := mysql.NewConnect(Uri)
	sql := "select * from `" + SchemaName + "`.`" + TableName + "` LIMIT 1"
	log.Println(sql)
	stmt, err := db.Prepare(sql)
	if err != nil {
		log.Fatal("Prepare err:", err)
		stmt.Close()
		return
	}
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	n := len(rows.Columns())
	m := make(map[string]interface{}, n)
	for {
		dest := make([]driver.Value, n, n)

		err := rows.Next(dest)
		if err != nil {
			//log.Println("ssssssssff err:",err)
			break
		}
		for i, v := range rows.Columns() {
			if dest[i] == nil {
				m[string(v)] = nil
				continue
			} else {
				m[string(v)] = dest[i]
			}
		}
		break
	}
	var noError bool = true

	for k, v := range m {
		if v == nil {
			log.Println(k, ":", "nil")
			continue
		} else {
			log.Println(k, ":", fmt.Sprint(v))
		}
		switch k {
		case "id", "testtimestamp":
			continue
			break
		default:
			if v != nil {
				log.Println(k, "is not null")
				noError = false
			} else {

			}
			break
		}
	}

	if noError == true {
		log.Println(" type and value is all right ")
	}
}

func TestChekcDataTypeByNull2(t *testing.T) {
	SchemaName := "bifrost_test"
	TableName := "binlog_field_test"
	Uri := "root:root@tcp(10.40.2.41:3306)/test"
	db, err := sql.Open("mysql", Uri)
	if err != nil {
		t.Fatal(err)
	}
	sql := "select id from `" + SchemaName + "`.`" + TableName + "` LIMIT 1"
	log.Println(sql)
	stmt, err := db.Prepare(sql)
	if err != nil {
		log.Fatal("Prepare err:", err)
		stmt.Close()
		return
	}
	//p := make([]driver.Value, 0)
	rows, err := stmt.Query()

	if err != nil {
		t.Fatal(err)
	}
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	n := len(columns)
	m := make(map[string]interface{}, n)
	for {
		//dest := make([]driver.Value, n, n)
		var id [][]byte
		rows.Scan(&id)
		bool := rows.Next()
		if bool == false {
			break
		}
		log.Println("id:", id)

		break
	}
	var noError bool = true

	for k, v := range m {
		if v == nil {
			log.Println(k, ":", "nil")
			continue
		} else {
			log.Println(k, ":", fmt.Sprint(v))
		}
		switch k {
		case "id", "testtimestamp":
			continue
			break
		default:
			if v != nil {
				log.Println(k, "is not null")
				noError = false
			} else {

			}
			break
		}
	}

	if noError == true {
		log.Println(" type and value is all right ")
	}
}
