package driver

var CanalSqlTypeMap map[string]int16

func init() {
	CanalSqlTypeMap = map[string]int16{
		"bool":       16,
		"json":       12,
		"bigint":     -5,
		"int":        -5,
		"mediumint":  4,
		"smallint":   5,
		"tinyint":    -6,
		"bit":        -7,
		"float":      6,
		"real":       7,
		"double":     8,
		"numeric":    2,
		"decimal":    3,
		"char":       1,
		"varchar":    12,
		"enum":       12,
		"set":        12,
		"text":       12,
		"blob":       12,
		"tinyblob":   12,
		"mediumblob": 2004,
		"longblob":   2004,
		"varbinary":  -2,
		"date":       91,
		"time":       92,
		"datetime":   93,
		"timestamp":  93,
		"year":       5,
		"other":      12,
	}
}

func GetCanalSqlTypeByDataType(dateType string) (sqlType int16) {
	if sqlType, ok := CanalSqlTypeMap[dateType]; ok {
		return sqlType
	} else {
		return 12
	}
}
