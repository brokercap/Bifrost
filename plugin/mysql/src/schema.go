package src

import (
	"database/sql/driver"
	"fmt"
	"github.com/brokercap/Bifrost/Bristol/mysql"
	"log"
	"strconv"
)

func NewMysqlDBConn(uri string) *mysqlDB {
	c := &mysqlDB{
		uri: uri,
	}
	c.Open()
	return c
}

type mysqlDB struct {
	uri  string
	conn mysql.MysqlConnection
	err  error
}

func (This *mysqlDB) Open() (b bool) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("[ERROR] output[%s] mysqlDB Open err:%+v \n", OutputName, err)
			This.err = fmt.Errorf(fmt.Sprint(err))
			b = false
		}
	}()
	This.conn = mysql.NewConnect(This.uri)
	return true
}

func (This *mysqlDB) Close() bool {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("[ERROR] output[%s] mysqlDB Close err:%+v \n", OutputName, err)
		}
	}()
	if This.conn != nil {
		This.conn.Close()
	}
	return true
}

func (This *mysqlDB) GetSchemaList() (data []string) {
	rows, err := This.conn.Query("SHOW DATABASES", []driver.Value{})
	if err != nil {
		log.Printf("[ERROR] output[%s] GetSchemaList err:%+v \n", OutputName, err)
		This.err = err
		return
	}
	defer rows.Close()
	filterMap := make(map[string]bool, 3)
	filterMap["performance_schema"] = true
	filterMap["information_schema"] = true
	filterMap["mysql"] = true

	for {
		dest := make([]driver.Value, 1, 1)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		var DataBase string
		DataBase = dest[0].(string)
		if _, ok := filterMap[DataBase]; ok {
			continue
		}
		data = append(data, DataBase)
	}
	return
}

func (This *mysqlDB) GetSchemaTableList(schema string) (data []string) {
	if schema == "" {
		return
	}
	sql := "SELECT TABLE_NAME FROM `information_schema`.`TABLES` WHERE TABLE_SCHEMA = ?"
	p := make([]driver.Value, 0)
	p = append(p, schema)
	rows, err := This.conn.Query(sql, p)
	if err != nil {
		log.Printf("[ERROR] output[%s] GetSchemaTableList schema:%s err:%+v \n", OutputName, schema, err)
		This.err = err
		return
	}
	defer rows.Close()

	for {
		dest := make([]driver.Value, 1, 1)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		data = append(data, dest[0].(string))
	}
	return
}

type TableStruct struct {
	COLUMN_NAME       string
	COLUMN_DEFAULT    *string
	IS_NULLABLE       string
	COLUMN_TYPE       string
	COLUMN_KEY        string
	EXTRA             string
	COLUMN_COMMENT    string
	DATA_TYPE         string
	NUMERIC_PRECISION *uint64
	NUMERIC_SCALE     *uint64
}

func (This *mysqlDB) GetTableFields(schema, table string) (data []TableStruct) {
	FieldList := make([]TableStruct, 0)
	sql := "SELECT `COLUMN_NAME`,`COLUMN_DEFAULT`,`IS_NULLABLE`,`COLUMN_TYPE`,`COLUMN_KEY`,`EXTRA`,`COLUMN_COMMENT`,`DATA_TYPE`,`NUMERIC_PRECISION`,`NUMERIC_SCALE` FROM `information_schema`.`columns` WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
	p := make([]driver.Value, 0)
	p = append(p, schema)
	p = append(p, table)
	rows, err := This.conn.Query(sql, p)
	if err != nil {
		return
	}
	defer rows.Close()
	if err != nil {
		log.Printf("[ERROR] output[%s] GetTableFields schema:%s table:%s err:%+v \n", OutputName, schema, table, err)
		return FieldList
	}
	for {
		dest := make([]driver.Value, 10, 10)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		var COLUMN_NAME string
		var COLUMN_DEFAULT *string
		var IS_NULLABLE string
		var COLUMN_TYPE string
		var COLUMN_KEY string
		var EXTRA string
		var COLUMN_COMMENT string
		var DATA_TYPE string
		var NUMERIC_PRECISION *uint64
		var NUMERIC_SCALE *uint64

		COLUMN_NAME = dest[0].(string)
		if dest[1] == nil {
			COLUMN_DEFAULT = nil
		} else {
			t := dest[1].(string)
			COLUMN_DEFAULT = &t
		}

		IS_NULLABLE = dest[2].(string)
		COLUMN_TYPE = dest[3].(string)
		COLUMN_KEY = dest[4].(string)
		EXTRA = dest[5].(string)
		COLUMN_COMMENT = dest[6].(string)
		DATA_TYPE = dest[7].(string)

		if dest[8] == nil {
			NUMERIC_PRECISION = nil
		} else {
			t, _ := strconv.ParseUint(fmt.Sprint(dest[8]), 10, 64)
			NUMERIC_PRECISION = &t
		}
		if dest[9] == nil {
			NUMERIC_SCALE = nil
		} else {
			t, _ := strconv.ParseUint(fmt.Sprint(dest[9]), 10, 64)
			NUMERIC_SCALE = &t
		}

		FieldList = append(FieldList, TableStruct{
			COLUMN_NAME:       COLUMN_NAME,
			COLUMN_DEFAULT:    COLUMN_DEFAULT,
			IS_NULLABLE:       IS_NULLABLE,
			COLUMN_TYPE:       COLUMN_TYPE,
			COLUMN_KEY:        COLUMN_KEY,
			EXTRA:             EXTRA,
			COLUMN_COMMENT:    COLUMN_COMMENT,
			DATA_TYPE:         DATA_TYPE,
			NUMERIC_PRECISION: NUMERIC_PRECISION,
			NUMERIC_SCALE:     NUMERIC_SCALE,
		})
	}
	return FieldList
}

func (This *mysqlDB) Begin() error {
	_, err := This.conn.Exec("BEGIN", make([]driver.Value, 0))
	return err
}

func (This *mysqlDB) Commit() error {
	_, err := This.conn.Exec("COMMIT", make([]driver.Value, 0))
	return err
}

func (This *mysqlDB) Rollback() error {
	_, err := This.conn.Exec("ROLLBACK", make([]driver.Value, 0))
	return err
}

func (This *mysqlDB) ShowTableCreate(schema, table string) string {
	sql := "SHOW CREATE TABLE `" + schema + "`.`" + table + "`"
	p := make([]driver.Value, 0)
	rows, err := This.conn.Query(sql, p)
	if err != nil {
		log.Printf("[ERROR] output[%s] ShowTableCreate schema:%s table:%s err:%+v \n", OutputName, schema, table, err)
		return ""
	}
	defer rows.Close()
	var createSQL string

	for {
		dest := make([]driver.Value, 2, 2)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		createSQL = dest[1].(string)
		break
	}
	return createSQL
}

func (This *mysqlDB) SelectVersion() string {
	sql := "SELECT version()"
	p := make([]driver.Value, 0)
	rows, err := This.conn.Query(sql, p)
	if err != nil {
		log.Printf("[ERROR] output[%s] SelectVersion err:%+v \n", OutputName, err)
		return ""
	}
	defer rows.Close()
	var version string

	for {
		dest := make([]driver.Value, 1, 1)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		version = dest[0].(string)
		break
	}
	return version
}

func (This *mysqlDB) CreateDatabase(database string) (err error) {
	sql := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", database)
	_, err = This.conn.Exec(sql, []driver.Value{})
	return
}

func (This *mysqlDB) Exec(sql string) (err error) {
	_, err = This.conn.Exec(sql, []driver.Value{})
	return
}

func (This *mysqlDB) ShowBackends() (backendsList []map[string]driver.Value, err error) {
	sql := "SHOW backends"
	p := make([]driver.Value, 0)
	rows, err := This.conn.Query(sql, p)
	if err != nil {
		log.Printf("[WARN] output[%s] ShowBackbends err:%+v \n", OutputName, err)
		return make([]map[string]driver.Value, 0), err
	}
	defer rows.Close()
	for {
		dest := make([]driver.Value, len(rows.Columns()), len(rows.Columns()))
		err := rows.Next(dest)
		if err != nil {
			break
		}
		m := make(map[string]driver.Value)
		for i, fieldName := range rows.Columns() {
			m[fieldName] = dest[i]
		}
		backendsList = append(backendsList, m)
	}
	return backendsList, nil
}
