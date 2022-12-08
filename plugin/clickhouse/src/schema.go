package src

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"log"
	"time"
)

type ckFieldStruct struct {
	Name              string
	Type              string
	DefaultType       string
	DefaultExpression string
}

func NewClickHouseDBConn(uri string) *ClickhouseDB {
	c := &ClickhouseDB{
		uri: uri,
	}
	c.Open()
	return c
}

type ClickhouseDB struct {
	uri  string
	conn clickhouse.Conn
	err  error
}

func (This *ClickhouseDB) GetConn() clickhouse.Conn {
	return This.conn
}

func (This *ClickhouseDB) Open() bool {
	log.Println("ClickHouse Uri:", This.uri)
	This.conn, This.err = clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "prod_xbt",
			Username: "default",
			Password: "Xbt123456!",
		},
		Debug: true,
		Debugf: func(format string, v ...interface{}) {
			fmt.Printf(format, v)
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		DialTimeout:      time.Duration(10) * time.Second,
		MaxOpenConns:     5,
		MaxIdleConns:     5,
		ConnMaxLifetime:  time.Duration(10) * time.Minute,
		ConnOpenStrategy: clickhouse.ConnOpenInOrder,
	})
	return true
}

func (This *ClickhouseDB) Close() bool {
	defer func() {
		if err := recover(); err != nil {
			log.Println("clickhouseDB close err:", err)
		}
	}()
	if This.conn != nil {
		This.conn.Close()
	}
	return true
}

func (This *ClickhouseDB) GetSchemaList() (data []string) {
	ctx := context.Background()
	//This.conn.Begin()
	rows, err := This.conn.Query(ctx, "SHOW DATABASES")
	if err != nil {
		This.err = err
		return
	}

	defer rows.Close()

	for rows.Next() {
		//过滤system库
		var (
			name string
		)
		if err := rows.Scan(&name); err != nil {
			log.Println("show data bases error")

		}
		if name == "system" {
			continue
		}
		data = append(data, name)
	}
	return
}

func (This *ClickhouseDB) GetSchemaTableList(schema string) (data []string) {
	if schema == "" {
		return
	}
	ctx := context.Background()

	rows, err := This.conn.Query(ctx, "select name from system.tables where database = '"+schema+"'")
	defer rows.Close()
	if err != nil {
		This.err = err
		return
	}

	for rows.Next() {
		var name string
		rows.Scan(&name)
		data = append(data, name)
	}
	return
}

func (This *ClickhouseDB) GetTableFields(SchemaName, TableName string) (data []ckFieldStruct) {
	ctx := context.Background()

	rows, err := This.conn.Query(ctx, "SELECT `name`,`type`,`default_kind`,`default_expression` FROM  system.columns where  `database` = '"+SchemaName+"' and `table` = '"+TableName+"'")

	if err != nil {
		This.err = err
		return
	}

	defer rows.Close()

	for rows.Next() {
		var (
			Name               string
			Type               string
			default_type       string
			default_expression string
		)
		rows.Scan(&Name, &Type, &default_type, &default_expression)
		data = append(data, ckFieldStruct{Name: Name, Type: Type, DefaultType: default_type, DefaultExpression: default_expression})
	}
	return
}

func (This *ClickhouseDB) GetVersion() (Version string) {
	ctx := context.Background()

	rows, err := This.conn.Query(ctx, "SELECT version()")

	if err != nil {
		This.err = err
		return
	}

	defer rows.Close()
	row := make([]driver.Value, 1)

	for rows.Next() {
		Version = row[0].(string)
	}
	return
}
