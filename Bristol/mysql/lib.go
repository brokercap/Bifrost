package mysql

import (
	"database/sql/driver"
	"fmt"
)

type tableStruct struct {
	SchemaName           string
	TableName            string
	Pri                  []string
	ColumnSchemaTypeList []*ColumnInfo
	needReload           bool
	ColumnMapping        map[string]string
}

type ColumnInfo struct {
	COLUMN_NAME            string
	COLLATION_NAME         string
	CHARACTER_SET_NAME     string
	COLUMN_COMMENT         string
	COLUMN_KEY             string
	COLUMN_TYPE            string
	NUMERIC_SCALE          string //浮点数精确多少数
	EnumValues             []string
	SetValues              []string
	IsBool                 bool
	IsPrimary              bool
	Unsigned               bool
	AutoIncrement          bool
	COLUMN_DEFAULT         string
	DATA_TYPE              string
	CHARACTER_OCTET_LENGTH uint64
}

type MysqlConnection interface {
	DumpBinlog(parser *eventParser, callbackFun callback) (driver.Rows, error)
	DumpBinlogGtid(parser *eventParser, callbackFun callback) (driver.Rows, error)
	Close() error
	Ping() error
	Prepare(query string) (driver.Stmt, error)
	Exec(query string, args []driver.Value) (driver.Result, error)
	Query(query string, args []driver.Value) (driver.Rows, error)
}

type EventReslut struct {
	Header         EventHeader
	Rows           []map[string]interface{}
	Query          string
	SchemaName     string
	TableName      string
	BinlogFileName string
	BinlogPosition uint32
	Gtid           string
	Pri            []string
	ColumnMapping  map[string]string
	EventID        uint64 // 事件ID
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

func StatusFlagName(t StatusFlag) (r string) {
	switch t {
	case STATUS_KILLED:
		r = "killed"
		break
	case STATUS_CLOSED:
		r = "close"
		break
	case STATUS_CLOSING:
		r = "closing"
		break
	case STATUS_STOPING:
		r = "stoping"
		break
	case STATUS_STOPED:
		r = "stop"
		break
	case STATUS_STARTING:
		r = "starting"
		break
	case STATUS_RUNNING:
		r = "running"
		break
	default:
		r = ""
		break
	}
	return
}
