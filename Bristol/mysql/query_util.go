package mysql

import (
	"database/sql/driver"
)

func GetResultByColumnName(rows driver.Rows, columns []string) (data []map[string]interface{}) {
	data = make([]map[string]interface{}, 0)
	n := len(columns)
	for {
		dest := make([]driver.Value, n, n)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		row := make(map[string]interface{}, 0)
		for i, columnName := range columns {
			row[columnName] = dest[i]
		}
	}
	return
}

func GetResult(rows driver.Rows) (data []map[string]interface{}) {
	return GetResultByColumnName(rows, rows.Columns())
}
