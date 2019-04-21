package src

import (
	"database/sql"
	_ "github.com/kshvakov/clickhouse"
)
func newClickHouseDBConn()  {

}

type clickhouseDB struct {
	uri string
	conn *sql.DB
	err error
}

func(This *clickhouseDB) Open() bool{
	This.conn, This.err = sql.Open("clickhouse", This.uri)
	return true
}

func (This *clickhouseDB) getTableInfo(TableName string)  {
}
