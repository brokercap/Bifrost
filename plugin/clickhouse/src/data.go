package src

import (
	"context"
	"database/sql/driver"
	driver2 "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"log"
	"time"
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

	for rows.Next() {
		d := make(map[string]driver.Value, 0)
		pointers := StrutToSliceOfFieldAddress(rows.ColumnTypes())
		err := rows.Scan(pointers...)
		if err != nil {
			log.Println("rows scan error.")
		}
		values := FieldAddressToValue(rows.ColumnTypes(), pointers)
		for i := 0; i < n; i++ {
			d[rows.Columns()[i]] = values[i]
		}
		data = append(data, d)
	}
	return data
}
func FieldAddressToValue(columns []driver2.ColumnType, points []interface{}) []driver.Value {

	values := make([]driver.Value, 0)
	for i := 0; i < len(columns); i++ {
		col := columns[i]
		name := col.ScanType().Name()
		switch name {
		case "int32":
			if ptr, ok := points[i].(*int32); ok {
				values = append(values, *ptr)
			}
			break
		case "uint32":
			if ptr, ok := points[i].(*uint32); ok {
				values = append(values, *ptr)
			}
			break
		case "uint8":
			if ptr, ok := points[i].(*uint8); ok {
				values = append(values, *ptr)
			}
			break
		case "int8":
			if ptr, ok := points[i].(*int8); ok {
				values = append(values, *ptr)
			}
			break
		case "int16":
			if ptr, ok := points[i].(*int16); ok {
				values = append(values, *ptr)
			}
			break
		case "uint16":
			if ptr, ok := points[i].(*uint16); ok {
				values = append(values, *ptr)
			}
			break
		case "int64":
			if ptr, ok := points[i].(*int64); ok {
				values = append(values, *ptr)
			}
			break
		case "uint64":
			if ptr, ok := points[i].(*uint64); ok {
				values = append(values, *ptr)
			}
			break
		case "string":
			if ptr, ok := points[i].(*string); ok {
				values = append(values, *ptr)
			}
			break
		case "Time":
			if ptr, ok := points[i].(*time.Time); ok {
				values = append(values, *ptr)
			}
			break
		case "float64":
			if ptr, ok := points[i].(*float64); ok {
				values = append(values, *ptr)
			}
			break
		}
	}
	return values
}
func StrutToSliceOfFieldAddress(columns []driver2.ColumnType) []interface{} {

	pointers := make([]interface{}, 0)

	for i := 0; i < len(columns); i++ {
		col := columns[i]
		name := col.ScanType().Name()
		switch name {
		case "int32":
			v := new(int32)
			pointers = append(pointers, v)
			break
		case "uint32":
			v := new(uint32)
			pointers = append(pointers, v)
			break
		case "uint8":
			v := new(uint8)
			pointers = append(pointers, v)
			break
		case "int8":
			v := new(int8)
			pointers = append(pointers, v)
			break
		case "int16":
			v := new(int16)
			pointers = append(pointers, v)
			break
		case "uint16":
			v := new(uint16)
			pointers = append(pointers, v)
			break
		case "int64":
			v := new(int64)
			pointers = append(pointers, v)
			break
		case "uint64":
			v := new(uint64)
			pointers = append(pointers, v)
			break
		case "string":
			v := new(string)
			pointers = append(pointers, v)
			break
		case "Time":
			v := new(time.Time)
			pointers = append(pointers, v)
			break
		case "float64":
			v := new(float64)
			pointers = append(pointers, v)
			break
		}
	}
	return pointers
}

func (This *ClickhouseDB) Exec(sql string, value []driver.Value) error {
	ctx := context.Background()
	if len(value) > 0 {
		batch, e := This.conn.PrepareBatch(ctx, sql)
		if e != nil {
			log.Println("click house PrepareBatch error.", sql)
			return e
		}
		var v interface{} = value
		e = batch.Append(v)
		if e != nil {
			return e
		}
		if err := batch.Send(); err != nil {
			log.Println("click house PrepareBatch error.", sql)
			return err
		}
	} else {
		err := This.conn.Exec(ctx, sql)
		if err != nil {
			return err
		}
	}
	return nil
}
