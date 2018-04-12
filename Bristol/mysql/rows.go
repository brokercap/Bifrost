package mysql

import (
	"database/sql/driver"
	"io"
)

type mysqlField struct {
	name      string
	fieldType FieldType
	flags     FieldFlag
}

type rowsContent struct {
	columns []mysqlField
	rows    []*[][]byte
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
		for i := 0; i < cap(dest); i++ {
			dest[i] = (*rows.content.rows[0])[i]
		}
		rows.content.rows = rows.content.rows[1:]
	} else {
		return io.EOF
	}
	return nil
}
