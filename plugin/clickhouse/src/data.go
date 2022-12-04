package src

import (
	"context"
	"database/sql/driver"
	"log"
)

func (This *ClickhouseDB) GetTableDataList(schema string, table string, where string) (data []map[string]driver.Value) {
	if schema == "" {
		return make([]map[string]driver.Value, 0)
	}

	//This.conn.Begin()
	//defer This.conn.Commit()
	sql := "select * from " + schema + "." + table + " where 1=1"
	if where != "" {
		sql += " and " + where
	}
	ctx := context.Background()
	rows, err := This.conn.Query(ctx, sql)
	if err != nil {
		This.err = err
		log.Println("click house Get table error.", err)
		return
	}
	defer rows.Close()
	data = make([]map[string]driver.Value, 0)
	n := len(rows.Columns())
	row := make([]driver.Value, n)

	for rows.Next() {
		d := make(map[string]driver.Value, 0)
		for i := 0; i < n; i++ {
			d[rows.Columns()[i]] = row[i]
		}
		data = append(data, d)
	}
	return data
}

func (This *ClickhouseDB) Exec(sql string, value []driver.Value) error {
	ctx := context.Background()
	batch, e := This.conn.PrepareBatch(ctx, sql)
	if e != nil {
		log.Println("click house PrepareBatch error.", sql)
		return e
	}
	e = batch.Append(value)
	if e != nil {
		return e
	}
	if err := batch.Send(); err != nil {
		log.Println("click house PrepareBatch error.", sql)
		return err
	}
	return nil
}
