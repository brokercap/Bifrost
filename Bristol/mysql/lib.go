package mysql

import (
	"database/sql/driver"
	"fmt"
)

type column_schema_type struct {
	COLUMN_NAME        string
	COLLATION_NAME     string
	CHARACTER_SET_NAME string
	COLUMN_COMMENT     string
	COLUMN_KEY         string
	COLUMN_TYPE        string
	NUMERIC_SCALE      string  //浮点数精确多少数
	enum_values        []string
	set_values         []string
	is_bool            bool
	is_primary         bool
	unsigned 		   bool
	auto_increment     bool
}

type MysqlConnection interface {
	DumpBinlog(filename string, position uint32, parser *eventParser, callbackFun callback, result chan error) (driver.Rows, error)
	Close() error
	Ping() error
	Prepare(query string) (driver.Stmt, error)
	Exec(query string,args []driver.Value)  (driver.Result, error)
}

type EventReslut struct {
	Header         EventHeader
	Rows           []map[string]interface{}
	Query          string
	SchemaName     string
	TableName      string
	BinlogFileName string
	BinlogPosition uint32
}

type callback func(data *EventReslut)

func fieldTypeName(t FieldType) string {
	switch t {
	case FIELD_TYPE_DECIMAL:
		return "FIELD_TYPE_DECIMAL"
	case FIELD_TYPE_TINY:
		return "FIELD_TYPE_TINY"
	case FIELD_TYPE_SHORT:
		return "FIELD_TYPE_SHORT"
	case FIELD_TYPE_LONG:
		return "FIELD_TYPE_LONG"
	case FIELD_TYPE_FLOAT:
		return "FIELD_TYPE_FLOAT"
	case FIELD_TYPE_DOUBLE:
		return "FIELD_TYPE_DOUBLE"
	case FIELD_TYPE_NULL:
		return "FIELD_TYPE_NULL"
	case FIELD_TYPE_TIMESTAMP:
		return "FIELD_TYPE_TIMESTAMP"
	case FIELD_TYPE_LONGLONG:
		return "FIELD_TYPE_LONGLONG"
	case FIELD_TYPE_INT24:
		return "FIELD_TYPE_INT24"
	case FIELD_TYPE_DATE:
		return "FIELD_TYPE_DATE"
	case FIELD_TYPE_TIME:
		return "FIELD_TYPE_TIME"
	case FIELD_TYPE_DATETIME:
		return "FIELD_TYPE_DATETIME"
	case FIELD_TYPE_YEAR:
		return "FIELD_TYPE_YEAR"
	case FIELD_TYPE_NEWDATE:
		return "FIELD_TYPE_NEWDATE"
	case FIELD_TYPE_VARCHAR:
		return "FIELD_TYPE_VARCHAR"
	case FIELD_TYPE_BIT:
		return "FIELD_TYPE_BIT"
	case FIELD_TYPE_NEWDECIMAL:
		return "FIELD_TYPE_NEWDECIMAL"
	case FIELD_TYPE_ENUM:
		return "FIELD_TYPE_ENUM"
	case FIELD_TYPE_SET:
		return "FIELD_TYPE_SET"
	case FIELD_TYPE_TINY_BLOB:
		return "FIELD_TYPE_TINY_BLOB"
	case FIELD_TYPE_MEDIUM_BLOB:
		return "FIELD_TYPE_MEDIUM_BLOB"
	case FIELD_TYPE_LONG_BLOB:
		return "FIELD_TYPE_LONG_BLOB"
	case FIELD_TYPE_BLOB:
		return "FIELD_TYPE_BLOB"
	case FIELD_TYPE_VAR_STRING:
		return "FIELD_TYPE_VAR_STRING"
	case FIELD_TYPE_STRING:
		return "FIELD_TYPE_STRING"
	case FIELD_TYPE_GEOMETRY:
		return "FIELD_TYPE_GEOMETRY"
	}
	return fmt.Sprintf("%d", t)
}