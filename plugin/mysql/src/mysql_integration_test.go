package src

import (
	dbDriver "database/sql/driver"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/brokercap/Bifrost/Bristol/mysql"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"github.com/brokercap/Bifrost/sdk/pluginTestData"
	"github.com/brokercap/Bifrost/server/history"
)

var url string = "root:root@tcp(127.0.0.1:55001)/bifrost_test?charset=utf8"

var SchemaName string = "bifrost_test"
var TableName string = "binlog_field_test"
var mysqlConn mysql.MysqlConnection

/*
ddl

CREATE TABLE `binlog_field_test` (
  `id0` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `id` int(11) unsigned DEFAULT NULL,
  `testtinyint` tinyint(4) NOT NULL DEFAULT '-1',
  `testsmallint` smallint(6) NOT NULL DEFAULT '-2',
  `testmediumint` mediumint(8) NOT NULL DEFAULT '-3',
  `testint` int(11) NOT NULL DEFAULT '-4',
  `testbigint` bigint(20) NOT NULL DEFAULT '-5',
  `testvarchar` varchar(400) NOT NULL DEFAULT 'var',
  `testchar` char(2) NOT NULL DEFAULT 'ch',
  `testenum` enum('en1','en2','en3') NOT NULL DEFAULT 'en1',
  `testset` set('set1','set2','set3') NOT NULL DEFAULT 'set1',
  `testtime` time NOT NULL DEFAULT '00:00:00',
  `testdate` date NOT NULL DEFAULT '0000-00-00',
  `testyear` year(4) NOT NULL DEFAULT '1989',
  `testtimestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `testdatetime` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `testfloat` float(9,2) NOT NULL DEFAULT '0.00',
  `testdouble` double(9,2) NOT NULL DEFAULT '0.00',
  `testdecimal` decimal(9,2) NOT NULL DEFAULT '0.00',
  `testtext` text,
  `testblob` blob,
  `testbit` bit(64) NOT NULL DEFAULT b'0',
  `testbool` tinyint(1) NOT NULL DEFAULT '0',
  `testmediumblob` mediumblob,
  `testlongblob` longblob,
  `testtinyblob` tinyblob,
  `test_unsinged_tinyint` tinyint(4) unsigned NOT NULL DEFAULT '1',
  `test_unsinged_smallint` smallint(6) unsigned NOT NULL DEFAULT '2',
  `test_unsinged_mediumint` mediumint(8) unsigned NOT NULL DEFAULT '3',
  `test_unsinged_int` int(11) unsigned NOT NULL DEFAULT '4',
  `test_unsinged_bigint` bigint(20) unsigned NOT NULL DEFAULT '5',
  `test_json` text,
  `event_type` varchar(10) DEFAULT '',
  PRIMARY KEY (`id0`)
) ENGINE=MyISAM AUTO_INCREMENT=0 DEFAULT CHARSET=utf8
*/

func TestChechUri_Integration(t *testing.T) {
	myConn := NewConn()
	myConn.SetOption(&url, nil)
	if err := myConn.CheckUri(); err != nil {
		log.Println("TestChechUri err:", err)
	} else {
		log.Println("TestChechUri success")
	}
}

func TestGetSchemaList_Integration(t *testing.T) {
	c := NewMysqlDBConn(url)
	defer c.Close()
	list := c.GetSchemaList()
	if len(list) > 0 {
		t.Log(list)
		t.Log("TestGetSchemaList success")
	} else {
		t.Error("TestGetSchemaList failed")
	}
}

func TestGetSchemaTableList_Integration(t *testing.T) {
	c := NewMysqlDBConn(url)
	defer c.Close()
	list := c.GetSchemaTableList(SchemaName)
	if len(list) > 0 {
		t.Log(list)
		t.Log("TestGetSchemaTableList success")
	} else {
		t.Error("TestGetSchemaTableList failed")
	}
}

func TestGetTableFields_Integration(t *testing.T) {
	c := NewMysqlDBConn(url)
	defer c.Close()
	list := c.GetTableFields(SchemaName, TableName)
	if len(list) > 0 {
		t.Log(list)
		t.Log("TestGetTableFields success")
	} else {
		t.Error("TestGetTableFields failed")
	}
}

func beforeTest() {

}

func checkMySQLSupportJson(db mysql.MysqlConnection) bool {
	stmt0, _ := db.Prepare("select version()")
	rows0, _ := stmt0.Query([]dbDriver.Value{})
	var MysqlVersion string
	for {
		dest := make([]dbDriver.Value, 1, 1)

		err := rows0.Next(dest)
		if err != nil {
			break
		}
		MysqlVersion = fmt.Sprint(dest[0])
		break
	}
	// 假如 mysql 版本 非 mysql5.7 及以上，不进行 json 类型测试
	bigVersionString := strings.Split(MysqlVersion, ".")[0]
	fmt.Println("bigVersionString:", bigVersionString)
	bigVersion, _ := strconv.Atoi(bigVersionString)
	fmt.Println("MysqlVersion[0:2]:", MysqlVersion[0:2])
	if bigVersion < 8 && MysqlVersion[0:2] != "5.7" {
		return false
	}
	return true
}

func getCreateTableSql(db mysql.MysqlConnection) string {
	if checkMySQLSupportJson(db) {
		return "CREATE TABLE  IF NOT EXISTS `" + SchemaName + "`.`" + TableName + "`( `id0` INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,`id` INT(11) UNSIGNED DEFAULT NULL , `testtinyint` TINYINT(4) NOT NULL DEFAULT '-1', `testsmallint` SMALLINT(6) NOT NULL DEFAULT '-2', `testmediumint` MEDIUMINT(8) NOT NULL DEFAULT '-3', `testint` INT(11) NOT NULL DEFAULT '-4', `testbigint` BIGINT(20) NOT NULL DEFAULT '-5', `testvarchar` VARCHAR(400) NOT NULL DEFAULT 'var', `testchar` CHAR(2) NOT NULL DEFAULT 'ch', `testenum` ENUM('en1', 'en2', 'en3') NOT NULL DEFAULT 'en1', `testset` SET('set1', 'set2', 'set3') NOT NULL DEFAULT 'set1', `testtime` TIME NOT NULL DEFAULT '00:00:00', `testdate` DATE NOT NULL DEFAULT '0000-00-00', `testyear` YEAR(4) NOT NULL DEFAULT '1989', `testtimestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, `testdatetime` DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00', `testfloat` FLOAT(9, 2) NOT NULL DEFAULT '0.00', `testdouble` DOUBLE(9, 2) NOT NULL DEFAULT '0.00', `testdecimal` DECIMAL(9, 2) NOT NULL DEFAULT '0.00', `testtext` TEXT DEFAULT NULL, `testblob` BLOB DEFAULT NULL, `testbit` BIT(64)  NOT NULL DEFAULT b'0', `testbool` TINYINT(1) NOT NULL DEFAULT '0', `testmediumblob` MEDIUMBLOB DEFAULT NULL, `testlongblob` LONGBLOB DEFAULT NULL, `testtinyblob` TINYBLOB DEFAULT NULL, `test_unsinged_tinyint` TINYINT(4) UNSIGNED NOT NULL DEFAULT '1', `test_unsinged_smallint` SMALLINT(6) UNSIGNED NOT NULL DEFAULT '2', `test_unsinged_mediumint` MEDIUMINT(8) UNSIGNED NOT NULL DEFAULT '3', `test_unsinged_int` INT(11) UNSIGNED NOT NULL DEFAULT '4', `test_unsinged_bigint` BIGINT(20) UNSIGNED NOT  NULL  DEFAULT '5',`test_json` json,`event_type` VARCHAR(10) DEFAULT '', PRIMARY KEY (`id0`) ) ENGINE = MYISAM AUTO_INCREMENT = 0 CHARSET = utf8"
	}
	return "CREATE TABLE  IF NOT EXISTS `" + SchemaName + "`.`" + TableName + "`( `id0` INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,`id` INT(11) UNSIGNED DEFAULT NULL , `testtinyint` TINYINT(4) NOT NULL DEFAULT '-1', `testsmallint` SMALLINT(6) NOT NULL DEFAULT '-2', `testmediumint` MEDIUMINT(8) NOT NULL DEFAULT '-3', `testint` INT(11) NOT NULL DEFAULT '-4', `testbigint` BIGINT(20) NOT NULL DEFAULT '-5', `testvarchar` VARCHAR(400) NOT NULL DEFAULT 'var', `testchar` CHAR(2) NOT NULL DEFAULT 'ch', `testenum` ENUM('en1', 'en2', 'en3') NOT NULL DEFAULT 'en1', `testset` SET('set1', 'set2', 'set3') NOT NULL DEFAULT 'set1', `testtime` TIME NOT NULL DEFAULT '00:00:00', `testdate` DATE NOT NULL DEFAULT '0000-00-00', `testyear` YEAR(4) NOT NULL DEFAULT '1989', `testtimestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, `testdatetime` DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00', `testfloat` FLOAT(9, 2) NOT NULL DEFAULT '0.00', `testdouble` DOUBLE(9, 2) NOT NULL DEFAULT '0.00', `testdecimal` DECIMAL(9, 2) NOT NULL DEFAULT '0.00', `testtext` TEXT DEFAULT NULL, `testblob` BLOB DEFAULT NULL, `testbit` BIT(64)  NOT NULL DEFAULT b'0', `testbool` TINYINT(1) NOT NULL DEFAULT '0', `testmediumblob` MEDIUMBLOB DEFAULT NULL, `testlongblob` LONGBLOB DEFAULT NULL, `testtinyblob` TINYBLOB DEFAULT NULL, `test_unsinged_tinyint` TINYINT(4) UNSIGNED NOT NULL DEFAULT '1', `test_unsinged_smallint` SMALLINT(6) UNSIGNED NOT NULL DEFAULT '2', `test_unsinged_mediumint` MEDIUMINT(8) UNSIGNED NOT NULL DEFAULT '3', `test_unsinged_int` INT(11) UNSIGNED NOT NULL DEFAULT '4', `test_unsinged_bigint` BIGINT(20) UNSIGNED NOT  NULL  DEFAULT '5',`test_json` text,`event_type` VARCHAR(10) DEFAULT '', PRIMARY KEY (`id0`) ) ENGINE = MYISAM AUTO_INCREMENT = 0 CHARSET = utf8"
}

func initDBTable(delTable bool) {
	c := mysql.NewConnect(url)
	sql1 := "CREATE DATABASE IF NOT EXISTS  `" + SchemaName + "`"
	_, err := c.Exec(sql1, []dbDriver.Value{})
	if err != nil {
		log.Fatal(err)
	}
	sql2 := getCreateTableSql(c)
	if delTable == false {
		_, err = c.Exec(sql2, []dbDriver.Value{})
		if err != nil {
			log.Fatal(err)
		}
	} else {
		sql3 := "DROP TABLE IF EXISTS `" + SchemaName + "`.`" + TableName + "`"
		_, err = c.Exec(sql3, []dbDriver.Value{})
		if err != nil {
			log.Fatal(err)
		}
		log.Println("sql2:", sql2)
		_, err = c.Exec(sql2, []dbDriver.Value{})
		if err != nil {
			log.Fatal(err)
		}
	}
	c.Close()
}

func getParam(SyncMode string) map[string]interface{} {
	type fieldStruct struct {
		ToField        string
		FromMysqlField string
	}

	param := make(map[string]interface{}, 0)
	Field := make([]fieldStruct, 0)
	Field = append(Field, fieldStruct{"id0", ""})
	Field = append(Field, fieldStruct{"id", "id"})
	Field = append(Field, fieldStruct{"test_unsinged_bigint", "test_unsinged_bigint"})
	Field = append(Field, fieldStruct{"test_unsinged_int", "test_unsinged_int"})
	Field = append(Field, fieldStruct{"test_unsinged_mediumint", "test_unsinged_mediumint"})
	Field = append(Field, fieldStruct{"test_unsinged_smallint", "test_unsinged_smallint"})
	Field = append(Field, fieldStruct{"test_unsinged_tinyint", "test_unsinged_tinyint"})
	Field = append(Field, fieldStruct{"testtinyint", "testtinyint"})
	Field = append(Field, fieldStruct{"testsmallint", "testsmallint"})
	Field = append(Field, fieldStruct{"testmediumint", "testmediumint"})
	Field = append(Field, fieldStruct{"testint", "testint"})
	Field = append(Field, fieldStruct{"testbigint", "testbigint"})
	Field = append(Field, fieldStruct{"testbit", "testbit"})
	Field = append(Field, fieldStruct{"testbool", "testbool"})
	Field = append(Field, fieldStruct{"testvarchar", "testvarchar"})
	Field = append(Field, fieldStruct{"testchar", "testchar"})
	Field = append(Field, fieldStruct{"testtime", "testtime"})
	Field = append(Field, fieldStruct{"testdate", "testdate"})
	Field = append(Field, fieldStruct{"testyear", "testyear"})

	Field = append(Field, fieldStruct{"testtimestamp", "testtimestamp"})

	Field = append(Field, fieldStruct{"testdatetime", "testdatetime"})
	Field = append(Field, fieldStruct{"testfloat", "testfloat"})
	Field = append(Field, fieldStruct{"testdouble", "testdouble"})
	Field = append(Field, fieldStruct{"testdecimal", "testdecimal"})
	Field = append(Field, fieldStruct{"testtext", "testtext"})
	Field = append(Field, fieldStruct{"testblob", "testblob"})
	Field = append(Field, fieldStruct{"testmediumblob", "testmediumblob"})
	Field = append(Field, fieldStruct{"testlongblob", "testlongblob"})
	Field = append(Field, fieldStruct{"testtinyblob", "testtinyblob"})
	Field = append(Field, fieldStruct{"testenum", "testenum"})
	Field = append(Field, fieldStruct{"testset", "testset"})
	Field = append(Field, fieldStruct{"event_type", "{$EventType}"})
	Field = append(Field, fieldStruct{"test_json", "test_json"})

	sql := ""
	for _, f := range Field {
		sql += f.ToField + ","
	}
	log.Println(sql)
	param["Field"] = Field

	PriKey := make([]fieldStruct, 1)
	PriKey[0] = fieldStruct{"id", "id"}
	param["PriKey"] = PriKey
	param["Schema"] = SchemaName
	param["Table"] = TableName
	param["SyncMode"] = SyncMode

	return param
}

func getPluginConn(SyncMode string) pluginDriver.Driver {
	myConn := NewConn()
	myConn.SetOption(&url, nil)
	myConn.Open()

	p, err := myConn.SetParam(getParam(SyncMode))
	if err != nil {
		log.Println("set param fatal err")
		log.Fatal(err)
	}

	log.Println("p:", p)
	return myConn
}

func TestCommit_Integration(t *testing.T) {

	beforeTest()
	conn := getPluginConn("Normal")
	initDBTable(true)
	t.Log("initDBTable success")

	e := pluginTestData.NewEvent()

	conn.Insert(e.GetTestInsertData(), false)
	conn.Del(e.GetTestDeleteData(), false)
	conn.Update(e.GetTestUpdateData(), false)
	conn.Insert(e.GetTestInsertData(), false)
	conn.Insert(e.GetTestInsertData(), false)
	conn.Insert(e.GetTestInsertData(), false)

	_, _, err2 := conn.TimeOutCommit()
	if err2 != nil {
		t.Fatal(err2)
	}
}

func TestInsertAndChekcData_Integration(t *testing.T) {
	beforeTest()
	initDBTable(true)
	conn := getPluginConn("Normal")
	e := pluginTestData.NewEvent()
	insertdata := e.GetTestInsertData()
	conn.Insert(insertdata, false)
	_, _, err2 := conn.TimeOutCommit()
	if err2 != nil {
		t.Fatal(err2)
	}

	checkResult, err := checkDataRight(insertdata.Rows[len(insertdata.Rows)-1])
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range checkResult["ok"] {
		t.Log(v)
	}

	for _, v := range checkResult["error"] {
		t.Error(v)
	}
}

func TestInsertNullAndChekcData_Integration(t *testing.T) {
	beforeTest()
	initDBTable(true)
	conn := getPluginConn("Normal")
	e := pluginTestData.NewEvent()
	e.SetIsNull(true)
	insertdata := e.GetTestInsertData()
	conn.Insert(insertdata, false)
	_, _, err2 := conn.TimeOutCommit()
	if err2 != nil {
		t.Fatal(err2)
	}

	checkResult, err := checkDataRight(insertdata.Rows[len(insertdata.Rows)-1])
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range checkResult["ok"] {
		t.Log(v)
	}

	for _, v := range checkResult["error"] {
		t.Error(v)
	}
}

func TestUpdateAndChekcData_Integration(t *testing.T) {
	beforeTest()
	initDBTable(false)
	conn := getPluginConn("Normal")
	e := pluginTestData.NewEvent()
	insertdata := e.GetTestInsertData()
	conn.Insert(insertdata, false)

	updateData := e.GetTestUpdateData()
	conn.Update(updateData, false)
	_, _, err2 := conn.TimeOutCommit()
	if err2 != nil {
		t.Fatal(err2)
	}

	checkResult, err := checkDataRight(updateData.Rows[len(updateData.Rows)-1])
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range checkResult["ok"] {
		t.Log(v)
	}

	for _, v := range checkResult["error"] {
		t.Error(v)
	}
}

func TestDelAndChekcData_Integration(t *testing.T) {
	beforeTest()
	initDBTable(true)
	conn := getPluginConn("Normal")
	e := pluginTestData.NewEvent()
	insertdata := e.GetTestInsertData()
	conn.Insert(insertdata, false)

	updateData := e.GetTestUpdateData()
	conn.Update(updateData, false)

	deleteData := e.GetTestDeleteData()
	conn.Del(deleteData, false)
	_, _, err2 := conn.TimeOutCommit()
	if err2 != nil {
		t.Fatal(err2)
	}
	m, err := getMysqlData(fmt.Sprint(deleteData.Rows[len(deleteData.Rows)-1]["id"]))
	if err != nil {
		t.Fatal(err)
	}

	if len(m) == 0 {
		t.Log("test delete success")
	} else {
		t.Error("test delete error,delete failed")
	}
}

func getMysqlConn() mysql.MysqlConnection {
	if mysqlConn == nil {
		mysqlConn = mysql.NewConnect(url)
	}
	return mysqlConn
}

func getMysqlData(id string) (map[string]interface{}, error) {
	schema := SchemaName
	table := TableName
	conn := getMysqlConn()
	Fields := history.GetSchemaTableFieldList(conn, schema, table)
	sql := ""
	for index, Field := range Fields {
		if index == 0 {
			sql = *Field.COLUMN_NAME
		} else {
			sql += "," + *Field.COLUMN_NAME
		}
	}
	sql = "select " + sql + " from `" + schema + "`.`" + table + "` where id = " + id

	stmt, err := conn.Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	rows, err := stmt.Query([]dbDriver.Value{})
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	n := len(Fields)
	m := make(map[string]interface{}, n)
	for {
		dest := make([]dbDriver.Value, n, n)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		for i, v := range Fields {
			if dest[i] == nil {
				m[*v.COLUMN_NAME] = nil
				continue
			}
			switch *v.DATA_TYPE {
			case "set":
				m[*v.COLUMN_NAME] = strings.Split(dest[i].(string), ",")
				break
			case "tinyint(1)":
				switch fmt.Sprint(dest[i]) {
				case "1":
					m[*v.COLUMN_NAME] = true
					break
				case "0":
					m[*v.COLUMN_NAME] = false
					break
				default:
					m[*v.COLUMN_NAME] = dest[i]
					break
				}
				break
			default:
				m[*v.COLUMN_NAME] = dest[i]
				break
			}
		}
		break
	}

	return m, nil
}

func getTableCount() (uint64, error) {
	conn := getMysqlConn()
	sql := "select count(*) from `" + SchemaName + "`.`" + TableName + "`"
	stmt, err := conn.Prepare(sql)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	rows, err := stmt.Query([]dbDriver.Value{})
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	dest := make([]dbDriver.Value, 1, 1)
	rows.Next(dest)
	uint64 := uint64(dest[0].(int64))
	return uint64, err

}

func checkDataRight(eventDataMap map[string]interface{}) (map[string][]string, error) {

	m, err := getMysqlData(fmt.Sprint(eventDataMap["id"]))
	if err != nil {
		return nil, err
	}

	result := make(map[string][]string, 0)
	result["ok"] = make([]string, 0)
	result["error"] = make([]string, 0)

	for key := range m {
		if key == "id0" {
			continue
		}
		if key == "event_type" {
			continue
		}
		var val interface{}
		var ok bool
		if val, ok = eventDataMap[key]; !ok {
			s := fmt.Sprint("field:", key, " not esxit")
			result["error"] = append(result["error"], s)
			continue
		}
		if reflect.TypeOf(val) == reflect.TypeOf(m[key]) && fmt.Sprint(val) == fmt.Sprint(m[key]) {
			s := fmt.Sprint(key, " == ", val, " ( ", reflect.TypeOf(val), " ) ")
			result["ok"] = append(result["ok"], s)
		} else {
			switch reflect.TypeOf(val).Kind() {
			case reflect.Map, reflect.Slice, reflect.Array:
				c, _ := json.Marshal(val)
				if string(c) == fmt.Sprint(c) {
					s := fmt.Sprint(key, " == ", val, " ( ", reflect.TypeOf(val), " ) ")
					result["ok"] = append(result["ok"], s)
				}
				break
			default:
				s := fmt.Sprint(key, " src: ", val, " ( ", reflect.TypeOf(val), " ) ", " != ", m[key], " ( ", reflect.TypeOf(m[key]), " )")
				result["error"] = append(result["error"], s)
			}
		}
	}
	return result, nil
}

func TestRandDataAndCheck_Integration(t *testing.T) {

	var n int = 1000

	e := pluginTestData.NewEvent()

	beforeTest()
	initDBTable(true)

	conn := getPluginConn("Normal")

	for i := 0; i < n; i++ {
		var eventData *pluginDriver.PluginDataType
		rand.Seed(time.Now().UnixNano() + int64(i))
		switch rand.Intn(3) {
		case 0:
			eventData = e.GetTestInsertData()
			conn.Insert(eventData, false)
			break
		case 1:
			eventData = e.GetTestUpdateData()
			conn.Update(eventData, false)
			break
		case 2:
			eventData = e.GetTestDeleteData()
			conn.Del(eventData, false)
			break
		case 3:
			eventData = e.GetTestQueryData()
			conn.Query(eventData, false)
			break
		}
	}
	conn.TimeOutCommit()

	count, err := getTableCount()
	if err != nil {
		t.Fatal(err)
	}

	if count != uint64(len(e.GetDataMap())) {
		for k, v := range e.GetDataMap() {
			t.Log(k, " ", v)
		}
		t.Fatal("mysql Table Count:", count, " != srcDataCount:", len(e.GetDataMap()))
	}

	for _, data := range e.GetDataMap() {
		checkResult, err := checkDataRight(data)
		if err != nil {
			t.Error("data:", data, "err:", err)
			continue
		}
		if len(checkResult["error"]) > 0 {
			t.Error("id:", data["id"], " failed")
			for _, v := range checkResult["error"] {
				t.Error(v)
			}
		} else {
			t.Log("id:", data["id"], data)
			t.Log("id:", data["id"], " success")
		}
	}

	t.Log("mysql Table Count:", count, " srcDataCount:", len(e.GetDataMap()))

	t.Log("test over")
}

func TestCommitBySymbol(t *testing.T) {

	url = "root:root@tcp(192.168.220.128:3308)/bifrost_test"
	beforeTest()
	TableName = "binlog_field_test4"
	conn := getPluginConn("Normal")
	initDBTable(false)

	e := pluginTestData.NewEvent()

	conn.Insert(e.GetTestInsertData(), false)
	conn.Del(e.GetTestDeleteData(), false)
	conn.Update(e.GetTestUpdateData(), false)
	conn.Insert(e.GetTestInsertData(), false)
	conn.Insert(e.GetTestInsertData(), false)
	conn.Insert(e.GetTestInsertData(), false)

	_, _, err2 := conn.TimeOutCommit()
	if err2 != nil {
		t.Fatal(err2)
	}
	t.Log("success")
}
