package mysql

import "strings"

func (parser *eventParser) getAutoTableSqlSchemaAndTable(name string) (SchemaName,TableName string) {
	dbAndTable := strings.Replace(name, "`", "", -1)
	i := strings.IndexAny(dbAndTable, ".")
	if i > 0 {
		SchemaName = dbAndTable[0:i]
		TableName = dbAndTable[i+1:]
	} else {
		TableName = dbAndTable
	}
	return
}

func (parser *eventParser) GetQueryTableName(sql string) (SchemaName,TableName string) {
	sql = strings.Trim(sql, " ")
	switch sql {
	case "COMMIT","BEGIN","commit","begin":
		return
	default:
		break
	}
	sqlUpper := strings.ToUpper(sql)

	// ALTER TABLE tableName
	// RENAME TABLE tableName
	// TRUNCATE TABLE tableName
	if strings.Index(sqlUpper,"ALTER TABLE") == 0 || strings.Index(sqlUpper,"RENAME TABLE") == 0 || strings.Index(sqlUpper,"TRUNCATE TABLE") == 0 {
		sqlArr := strings.Split(sql, " ")
		SchemaName,TableName = parser.getAutoTableSqlSchemaAndTable(sqlArr[2])
		return
	}

	// DROP TABLE IF EXISTS tableName
	if strings.Index(sqlUpper,"DROP TABLE") == 0 {
		sqlArr := strings.Split(sql, " ")
		var tableNameIndex = 2
		if strings.Index(sqlUpper,"IF EXISTS") > 0 {
			tableNameIndex = 4
		}
		SchemaName,TableName = parser.getAutoTableSqlSchemaAndTable(sqlArr[tableNameIndex])
		return
	}

	// CREATE TABLE IF NOT EXISTS `tableName` (
	// CREATE TABLE IF NOT EXISTS `tableName`(
	// CREATE TABLE IF `tableName`(
	if strings.Index(sqlUpper,"CREATE TABLE") == 0 {
		sqlArr := strings.Split(sql, " ")

		// 假如 存在 IF NOT EXISTS 则代表表名是按 空格分割过后的数组里 第6个，也就是下标 5
		var tableNameIndex = 2
		if strings.Index(sqlUpper,"IF NOT EXISTS") > 0 {
			tableNameIndex = 5
		}
		//create table table(id int) 这种表名和( 相挨着的情况
		if strings.Index(sqlArr[tableNameIndex],"(") > 0 {
			tmpTableName := strings.Split(sqlArr[tableNameIndex], "(")[0]
			SchemaName,TableName = parser.getAutoTableSqlSchemaAndTable(tmpTableName)
		}else{
			SchemaName,TableName = parser.getAutoTableSqlSchemaAndTable(sqlArr[tableNameIndex])
		}
		return
	}

	// CREATE DATABASE IF NOT EXISTS databaseName
	if strings.Index(sqlUpper,"CREATE DATABASE") == 0 {
		sqlArr := strings.Split(sql, " ")
		if strings.Index(sqlUpper,"IF NOT EXISTS") < 0 {
			SchemaName = sqlArr[2]
		}else{
			SchemaName = sqlArr[5]
		}
		return
	}

	// DROP DATABASE IF EXISTS databaseName
	if strings.Index(sqlUpper,"DROP DATABASE") == 0 {
		sqlArr := strings.Split(sql, " ")
		if strings.Index(sqlUpper,"IF EXISTS") < 0 {
			SchemaName = sqlArr[2]
		}else{
			SchemaName = sqlArr[4]
		}
		return
	}else{
		// UPDATE Table
		// INSERT INTO Table
		// DELETE FROM Table
		// REPLACE INTO Table
		var tableNameIndex = 1
		switch sqlUpper[0:6] {
		case "UPDATE":
			break
		case "INSERT","DELETE","REPLAC":
			tableNameIndex = 2
			break
		default:
			return
		}
		sqlArr := strings.Split(sql, " ")
		tmpTableName := sqlArr[tableNameIndex]
		if strings.Index(tmpTableName,"(") > 0 {
			tmpTableName = strings.Split(tmpTableName,"(")[0]
			SchemaName,TableName = parser.getAutoTableSqlSchemaAndTable(tmpTableName)
		}else{
			SchemaName,TableName = parser.getAutoTableSqlSchemaAndTable(tmpTableName)
		}
	}
	return
}
