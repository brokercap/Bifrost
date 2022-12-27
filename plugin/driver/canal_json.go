package driver

import (
	"encoding/json"
	"strings"
)

type PluginDataCanal struct {
	Data      []map[string]interface{} `json:"data"`
	Old       []map[string]interface{} `json:"old"`
	Database  string                   `json:"database"`
	Table     string                   `json:"table"`
	Es        int64                    `json:"es"`
	Ts        int64                    `json:"ts"`
	IsDDL     bool                     `json:"isDdL"`
	Sql       string                   `json:"sql"`
	MysqlType map[string]string        `json:"mysqlType"`
	SqlType   map[string]int16         `json:"sqltype"`
	Type      string                   `json:"type"`
	PkNames   []string                 `json:"pkNames"`
}

func NewPluginDataCanal(content []byte) (*PluginDataCanal, error) {
	var c PluginDataCanal
	err := json.Unmarshal(content, &c)
	return &c, err
}

func (c *PluginDataCanal) ToBifrostOutputPluginData() (data *PluginDataType) {
	if c.Sql != "" {
		data = c.ToBifrostOutputPluginDataWithSql()
	} else {
		data = c.ToBifrostOutputPluginDataWithRow()
	}
	data.SchemaName = c.Database
	data.TableName = c.Table
	return
}

func (c *PluginDataCanal) ToBifrostOutputPluginDataWithSql() *PluginDataType {
	data := &PluginDataType{
		EventType: "sql",
		Query:     c.Sql,
	}
	return data
}

func (c *PluginDataCanal) ToBifrostOutputPluginDataWithRow() *PluginDataType {
	var columnMapping = make(map[string]string, len(c.MysqlType))
	for name, mysqlType := range c.MysqlType {
		var columnMappingType string
		var unsigned bool
		if strings.Contains(mysqlType, "unsigned") {
			unsigned = true
		}
		var dataType string
		dataType = strings.Split(strings.Trim(strings.ReplaceAll(mysqlType, "unsigned", ""), " "), "(")[0]

		switch dataType {
		case "tinyint":
			if unsigned {
				columnMappingType = "uint8"
			} else {
				if mysqlType == "tinyint(1)" {
					columnMappingType = "bool"
				} else {
					columnMappingType = "int8"
				}
			}
		case "smallint":
			if unsigned {
				columnMappingType = "uint16"
			} else {
				columnMappingType = "int16"
			}
		case "mediumint":
			if unsigned {
				columnMappingType = "uint24"
			} else {
				columnMappingType = "int24"
			}
		case "int":
			if unsigned {
				columnMappingType = "uint32"
			} else {
				columnMappingType = "int32"
			}
		case "bigint":
			if unsigned {
				columnMappingType = "uint64"
			} else {
				columnMappingType = "int64"
			}
		case "numeric":
			columnMappingType = strings.Replace(mysqlType, "numeric", "decimal", 1)
		case "real":
			columnMappingType = strings.Replace(mysqlType, "real", "double", 1)
		default:
			columnMappingType = mysqlType
			break
		}
		columnMapping[name] = "Nullable(" + columnMappingType + ")"
	}

	for _, name := range c.PkNames {
		columnMappingType := columnMapping[name]
		columnMappingType = columnMappingType[9 : len(columnMappingType)-1]
		columnMapping[name] = columnMappingType
	}
	var rows []map[string]interface{}
	var eventType string
	switch c.Type {
	case "INSERT":
		rows = c.ToBifrostOutputPluginDataWithInsertRow()
		eventType = "insert"
	case "DELETE":
		rows = c.ToBifrostOutputPluginDataWithDeleteRow()
		eventType = "delete"
	case "UPDATE":
		rows = c.ToBifrostOutputPluginDataWithUpdateRow()
		eventType = "update"
	}
	data := &PluginDataType{
		EventType:     eventType,
		Rows:          rows,
		Pri:           c.PkNames,
		ColumnMapping: columnMapping,
	}
	return data
}

func (c *PluginDataCanal) ToBifrostOutputPluginDataWithInsertRow() (rows []map[string]interface{}) {
	return c.Data
}

func (c *PluginDataCanal) ToBifrostOutputPluginDataWithUpdateRow() (rows []map[string]interface{}) {
	for i, row := range c.Data {
		rows = append(rows, c.Old[i])
		rows = append(rows, row)
	}
	return rows
}

func (c *PluginDataCanal) ToBifrostOutputPluginDataWithDeleteRow() (rows []map[string]interface{}) {
	return c.Data
}
