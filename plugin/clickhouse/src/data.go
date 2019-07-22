package src

import "database/sql/driver"

func (This *clickhouseDB) GetTableDataList(schema string,table string,where string) (data []map[string]driver.Value) {
	if schema == ""{
		return make([]map[string]driver.Value,0)
	}

	This.conn.Begin()
	stmt, err := This.conn.Prepare("select * from "+schema+"."+table+" where 1=1 and "+where)
	if err == nil{
		defer stmt.Close()
	}
	rows, err := stmt.Query([]driver.Value{})
	if err != nil {
		This.err = err
		return
	}
	defer rows.Close()
	data = make([]map[string]driver.Value,0)
	n := len(rows.Columns())
	row := make([]driver.Value, n)

	for rows.Next(row) == nil {
		d := make(map[string]driver.Value,0)
		for i:=0;i<n;i++{
			d[rows.Columns()[i]] = row[i]
		}
		data = append(data,d)
	}
	This.conn.Commit()
	return data
}
