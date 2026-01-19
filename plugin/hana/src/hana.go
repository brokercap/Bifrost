package src

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	_ "github.com/SAP/go-hdb/driver"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
)

const OutputName = "hana"
const VERSION = "v1.0.0"
const BIFROST_VERION = "v1.0.0"

func init() {
	pluginDriver.Register(OutputName, NewConn, VERSION, BIFROST_VERION)
}

type Conn struct {
	uri    *string
	status string
	p      *PluginParam
	conn   *sql.DB
	err    error
}

type PluginParam struct {
	Schema    string
	Table     string
	BatchSize int
	Data      *TableDataStruct
}

type TableDataStruct struct {
	Data       []*pluginDriver.PluginDataType
	CommitData []*pluginDriver.PluginDataType
}

func NewConn() pluginDriver.Driver {
	return &Conn{status: "close"}
}

func (c *Conn) SetOption(uri *string, param map[string]interface{}) {
	c.uri = uri
}

func (c *Conn) Open() error {
	c.Connect()
	return nil
}

func (c *Conn) Connect() bool {
	var err error
	if c.uri == nil {
		c.err = fmt.Errorf("uri is nil")
		return false
	}
	c.conn, err = sql.Open("hdb", *c.uri)
	if err != nil {
		c.err = err
		return false
	}
	err = c.conn.Ping()
	if err != nil {
		c.err = err
		return false
	}
	return true
}

func (c *Conn) Close() bool {
	if c.conn != nil {
		c.conn.Close()
	}
	return true
}

func (c *Conn) CheckUri() error {
	c.Connect()
	return c.err
}

func (c *Conn) GetUriExample() string {
	return "hdb://user:password@host:port"
}

func (c *Conn) SetParam(p interface{}) (interface{}, error) {
	if p == nil {
		return nil, fmt.Errorf("param is nil")
	}
	switch p.(type) {
	case *PluginParam:
		c.p = p.(*PluginParam)
		return p, nil
	default:
		return c.GetParam(p)
	}
}

func (c *Conn) GetParam(p interface{}) (*PluginParam, error) {
	s, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	var param PluginParam
	err2 := json.Unmarshal(s, &param)
	if err2 != nil {
		return nil, err2
	}
	if param.BatchSize == 0 {
		param.BatchSize = 500
	}
	param.Data = &TableDataStruct{
		Data:       make([]*pluginDriver.PluginDataType, 0),
		CommitData: make([]*pluginDriver.PluginDataType, 0),
	}
	c.p = &param
	return c.p, nil
}

func (c *Conn) Insert(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return c.sendToCacheList(data, retry)
}

func (c *Conn) Update(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return c.sendToCacheList(data, retry)
}

func (c *Conn) Del(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return c.sendToCacheList(data, retry)
}

func (c *Conn) Query(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return nil, nil, nil
}

func (c *Conn) Commit(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return nil, nil, nil
}

func (c *Conn) TimeOutCommit() (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return c.AutoCommit()
}

func (c *Conn) Skip(data *pluginDriver.PluginDataType) error {
	return nil
}

func (c *Conn) sendToCacheList(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	c.p.Data.Data = append(c.p.Data.Data, data)
	if len(c.p.Data.Data) >= c.p.BatchSize {
		return c.AutoCommit()
	}
	return nil, nil, nil
}

func (c *Conn) AutoCommit() (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	if len(c.p.Data.Data) == 0 {
		return nil, nil, nil
	}
	list := c.p.Data.Data
	c.p.Data.Data = make([]*pluginDriver.PluginDataType, 0)

	// Group data by table
	dataMap := make(map[string][]*pluginDriver.PluginDataType)
	for _, d := range list {
		key := c.getSchemaTable(d)
		dataMap[key] = append(dataMap[key], d)
	}

	for _, tableData := range dataMap {
		if err := c.commitTableData(tableData); err != nil {
			return nil, tableData[0], err
		}
	}

	lastData := list[len(list)-1]
	return lastData, nil, nil
}

func (c *Conn) commitTableData(list []*pluginDriver.PluginDataType) error {
	tx, err := c.conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, data := range list {
		schemaName, tableName := c.getSchemaTableName(data)
		switch data.EventType {
		case "insert":
			err = c.executeInsert(tx, schemaName, tableName, data)
		case "update":
			err = c.executeUpdate(tx, schemaName, tableName, data)
		case "delete":
			err = c.executeDelete(tx, schemaName, tableName, data)
		}
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (c *Conn) executeInsert(tx *sql.Tx, schema, table string, data *pluginDriver.PluginDataType) error {
	if len(data.Rows) == 0 {
		return nil
	}
	row := data.Rows[len(data.Rows)-1]

	cols := make([]string, 0, len(row))
	vals := make([]interface{}, 0, len(row))
	placeholders := make([]string, 0, len(row))

	for k, v := range row {
		cols = append(cols, fmt.Sprintf("\"%s\"", k))
		val, err := c.convertValue(v)
		if err != nil {
			return err
		}
		vals = append(vals, val)
		placeholders = append(placeholders, "?")
	}

	query := fmt.Sprintf("INSERT INTO \"%s\".\"%s\" (%s) VALUES (%s)", schema, table, strings.Join(cols, ","), strings.Join(placeholders, ","))
	_, err := tx.Exec(query, vals...)
	return err
}

func (c *Conn) executeUpdate(tx *sql.Tx, schema, table string, data *pluginDriver.PluginDataType) error {
	if len(data.Rows) < 2 {
		return fmt.Errorf("update rows length < 2")
	}
	newRow := data.Rows[1]
	if len(data.Pri) == 0 {
		return fmt.Errorf("no primary key found for update")
	}

	setClauses := make([]string, 0)
	vals := make([]interface{}, 0)

	for k, v := range newRow {
		setClauses = append(setClauses, fmt.Sprintf("\"%s\" = ?", k))
		val, err := c.convertValue(v)
		if err != nil {
			return err
		}
		vals = append(vals, val)
	}

	whereClauses := make([]string, 0)
	for _, pri := range data.Pri {
		whereClauses = append(whereClauses, fmt.Sprintf("\"%s\" = ?", pri))
		if val, ok := data.Rows[0][pri]; ok {
			v, err := c.convertValue(val)
			if err != nil {
				return err
			}
			vals = append(vals, v)
		} else {
			return fmt.Errorf("primary key %s not found in old row", pri)
		}
	}

	query := fmt.Sprintf("UPDATE \"%s\".\"%s\" SET %s WHERE %s", schema, table, strings.Join(setClauses, ","), strings.Join(whereClauses, " AND "))
	_, err := tx.Exec(query, vals...)
	return err
}

func (c *Conn) executeDelete(tx *sql.Tx, schema, table string, data *pluginDriver.PluginDataType) error {
	if len(data.Rows) == 0 {
		return fmt.Errorf("delete rows length 0")
	}
	row := data.Rows[0]
	if len(data.Pri) == 0 {
		return fmt.Errorf("no primary key found for delete")
	}

	whereClauses := make([]string, 0)
	vals := make([]interface{}, 0)

	for _, pri := range data.Pri {
		whereClauses = append(whereClauses, fmt.Sprintf("\"%s\" = ?", pri))
		if val, ok := row[pri]; ok {
			v, err := c.convertValue(val)
			if err != nil {
				return err
			}
			vals = append(vals, v)
		} else {
			return fmt.Errorf("primary key %s not found in row", pri)
		}
	}

	query := fmt.Sprintf("DELETE FROM \"%s\".\"%s\" WHERE %s", schema, table, strings.Join(whereClauses, " AND "))
	_, err := tx.Exec(query, vals...)
	return err
}

func (c *Conn) getSchemaTable(data *pluginDriver.PluginDataType) string {
	schema, table := c.getSchemaTableName(data)
	return schema + "." + table
}

func (c *Conn) getSchemaTableName(data *pluginDriver.PluginDataType) (string, string) {
	schema := c.p.Schema
	if schema == "" {
		schema = data.SchemaName
	}
	table := c.p.Table
	if table == "" {
		table = data.TableName
	}
	return schema, table
}

func (c *Conn) convertValue(v interface{}) (interface{}, error) {
	if v == nil {
		return nil, nil
	}
	switch v.(type) {
	case string:
		return v, nil
	case int, int8, int16, int32, int64:
		return v, nil
	case uint, uint8, uint16, uint32, uint64:
		return v, nil
	case float32, float64:
		return v, nil
	case bool:
		return v, nil
	case []byte:
		return string(v.([]byte)), nil
	case time.Time:
		return v, nil
	case json.Number:
		return v.(json.Number).String(), nil
	default:
		// Fallback to string representation for unknown types
		// Check if it's a map/slice, convert to JSON string
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Map || rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array || rv.Kind() == reflect.Struct {
			b, err := json.Marshal(v)
			if err != nil {
				return nil, err
			}
			return string(b), nil
		}
		return fmt.Sprintf("%v", v), nil
	}
}
