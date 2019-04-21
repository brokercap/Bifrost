package src

import (
	"database/sql"
	_ "github.com/kshvakov/clickhouse"
	"os/exec"
	"os"
	"path/filepath"
	"github.com/jc3wish/Bifrost/manager/xgo"
	"net/http"
)

var execDir string

func init()  {
	xgo.AddRoute("/bifrost/clickhouse/tableinfo",getClickHouseTableFields)
	execPath, _ := exec.LookPath(os.Args[0])
	execDir = filepath.Dir(execPath)+"/"
}

func getClickHouseTableFields(w http.ResponseWriter,req *http.Request)  {

}

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

func (This *clickhouseDB) getTableFields(TableName string) (data map[string]string) {
	rows,err := This.conn.Query("DESC TABLE "+TableName)
	if err != nil {
		This.err = err
		return
	}
	defer rows.Close()
	for rows.Next() {
		var (
			Name               string
			Type           	string
		)
		if err := rows.Scan(&Name, &Type); err != nil {
			This.err = err
			return
		}
		data[Name] = Type
	}
	return
}
