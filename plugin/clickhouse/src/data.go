package src

import "database/sql/driver"

func (This *ClickhouseDB) GetTableDataList(schema string, table string, where string) (data []map[string]driver.Value) {
	if schema == "" {
		return make([]map[string]driver.Value, 0)
	}

	This.conn.Begin()
	defer This.conn.Commit()
	sql := "select * from " + schema + "." + table + " where 1=1"
	if where != "" {
		sql += " and " + where
	}
	stmt, err := This.conn.Prepare(sql)
	if err == nil {
		defer stmt.Close()
	}
	rows, err := stmt.Query([]driver.Value{})
	if err != nil {
		This.err = err
		return
	}
	defer rows.Close()
	data = make([]map[string]driver.Value, 0)
	n := len(rows.Columns())
	row := make([]driver.Value, n)

	for rows.Next(row) == nil {
		d := make(map[string]driver.Value, 0)
		for i := 0; i < n; i++ {
			d[rows.Columns()[i]] = row[i]
		}
		data = append(data, d)
	}
	return data
}

func (This *ClickhouseDB) Exec(sql string, value []driver.Value) error {
	This.conn.Begin()
	stmt, e := This.conn.Prepare(sql)
	if e != nil {
		This.conn.Commit()
		return e
	}
	defer stmt.Close()
	_, e = stmt.Exec(value)
	if e != nil {
		This.conn.Commit()
		return e
	}
	This.conn.Commit()
	return nil
}
