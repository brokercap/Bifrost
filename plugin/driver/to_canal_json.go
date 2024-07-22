package driver

import (
	"strings"
	"time"
)

func (c *PluginDataType) ToCanalJsonObject() (canal *PluginDataCanal, err error) {
	canal = &PluginDataCanal{
		Data:      nil,
		Old:       nil,
		Database:  c.SchemaName,
		Table:     c.TableName,
		Es:        int64(c.Timestamp) * 1000,
		Ts:        time.Now().Unix() * 1000,
		IsDDL:     c.IsDDL(),
		Sql:       c.Query,
		MysqlType: nil,
		SqlType:   nil,
		Type:      strings.ToUpper(c.EventType),
		PkNames:   c.Pri,
	}
	switch c.EventType {
	case "sql", "commit":
		return
	case "update":
		var data = make([]map[string]interface{}, len(c.Rows)/2)
		var Old = make([]map[string]interface{}, len(c.Rows)/2)
		for i, row := range c.Rows {
			if i&1 == 0 {
				// 偶数
				Old[i/2] = row
			} else {
				// 奇数
				data[i/2] = row
			}
		}
		canal.Data = data
		canal.Old = Old
		break
	case "insert", "delete":
		canal.Data = c.Rows
		break
	}
	canal.MysqlType, canal.SqlType = c.ToCanalJsonMysqlAndSqlType()
	return
}

func (c *PluginDataType) ToCanalJsonMysqlAndSqlType() (map[string]string, map[string]int16) {
	if c.ColumnMapping == nil {
		return nil, nil
	}
	mysqlTypeMap := make(map[string]string, 0)
	sqlTypeMap := make(map[string]int16, 0)
	for name, dataType := range c.ColumnMapping {
		var dataTypeLen = len(dataType)
		var columnType string
		if dataType[0:3] == "Nul" && dataTypeLen > 9 {
			columnType = dataType[9 : len(dataType)-1]
		}
		var sqlType int16
		switch dataType {
		case "bool":
			columnType = "tinyint(1)"
			sqlType = GetCanalSqlTypeByDataType("bool")
			break
		case "int8":
			columnType = "tinyint(4)"
			sqlType = GetCanalSqlTypeByDataType("tinyint")
			break

		case "int16":
			columnType = "smallint(6)"
			sqlType = GetCanalSqlTypeByDataType("smallint")
			break
		case "uint16":
			columnType = "smallint(6) unsigned"
			sqlType = GetCanalSqlTypeByDataType("smallint")
			break

		case "int24":
			columnType = "mediumint(8)"
			sqlType = GetCanalSqlTypeByDataType("mediumint")
			break
		case "uint24":
			columnType = "mediumint(8) unsigned"
			sqlType = GetCanalSqlTypeByDataType("mediumint")
			break

		case "int", "int32":
			columnType = "int(11)"
			sqlType = GetCanalSqlTypeByDataType("int")
			break
		case "uint", "uint32":
			columnType = "int(11) unsigned"
			sqlType = GetCanalSqlTypeByDataType("int")
			break

		case "int64":
			columnType = "bigint(20)"
			sqlType = GetCanalSqlTypeByDataType("bigint")
			break
		case "uint64":
			columnType = "bigint(20) unsigned"
			sqlType = GetCanalSqlTypeByDataType("bigint")
			break
		default:
			mysqlDataType := strings.ToLower(strings.Trim(strings.Split(columnType, "(")[0], " "))
			sqlType = GetCanalSqlTypeByDataType(mysqlDataType)
			break
		}
		mysqlTypeMap[name] = columnType
		sqlTypeMap[name] = sqlType
	}
	return mysqlTypeMap, sqlTypeMap
}
