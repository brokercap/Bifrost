package mysql

import (
	"database/sql/driver"
	"io"
)

type mysqlField struct {
	name      string
	fieldType FieldType
	flags     FieldFlag
	length    uint32
}

type rowsContent struct {
	columns []mysqlField
	//rows    []*[][]byte
	rows [][]driver.Value
}

type mysqlRows struct {
	content *rowsContent
}

func (rows mysqlRows) Columns() (columns []string) {
	columns = make([]string, len(rows.content.columns))
	for i := 0; i < cap(columns); i++ {
		columns[i] = rows.content.columns[i].name
	}
	return
}

func (rows mysqlRows) Close() error {
	rows.content = nil
	return nil
}

// Next returns []driver.Value filled with either nil values for NULL entries
// or []byte's for all other entries. Type conversion is done on rows.scan(),
// when the dest. type is know, which makes type conversion easier and avoids
// unnecessary conversions.
func (rows mysqlRows) Next(dest []driver.Value) error {
	if len(rows.content.rows) > 0 {
		var n int
		if len(rows.content.rows[0]) >= cap(dest) {
			n = cap(dest)
		} else {
			n = len(rows.content.rows[0])
		}
		for i := 0; i < n; i++ {
			dest[i] = rows.content.rows[0][i]
			/*
				rows.content.rows[0][i] ==
				if (*rows.content.rows[0])[i] == nil{
					dest[i] = nil
				}else{
					dest[i] = (*rows.content.rows[0])[i]
				}
			*/
		}
		rows.content.rows = rows.content.rows[1:]
	} else {
		return io.EOF
	}
	return nil
}
