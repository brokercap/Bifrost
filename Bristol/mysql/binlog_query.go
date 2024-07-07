package mysql

import (
	"regexp"
	"strings"
)

func (parser *eventParser) getAutoTableSqlSchemaAndTable(name string) (SchemaName, TableName string) {
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

func (parser *eventParser) GetQueryTableName(sql string) (SchemaName, TableName string, noReloadTableInfo bool, isDDL bool) {
	sql = strings.Trim(sql, " ")
	switch sql {
	case "COMMIT", "BEGIN", "commit", "begin":
		return
	default:
		break
	}
	isDDL = true

	//将换行去除
	sql = strings.ReplaceAll(sql, "\r\n", "")
	sql = strings.ReplaceAll(sql, "\n", "")
	sql = strings.ReplaceAll(sql, "\r", "")
	sql = TransferNotes2Space(sql)
	//去除连续的两个空格
	for {
		if strings.Index(sql, "  ") >= 0 {
			sql = strings.ReplaceAll(sql, "  ", " ")
		} else {
			break
		}
	}
	for {
		if strings.Index(sql, "	") >= 0 {
			sql = strings.ReplaceAll(sql, "	", " ") // 这两个是不一样的，一个是两个 " "+" "，一个是" "+""
		} else {
			break
		}
	}
	sqlUpper := strings.ToUpper(sql)

	// ALTER TABLE tableName
	// TRUNCATE TABLE tableName
	if strings.Index(sqlUpper, "ALTER TABLE ") == 0 || strings.Index(sqlUpper, "TRUNCATE TABLE ") == 0 {
		sqlArr := strings.Split(sql, " ")
		SchemaName, TableName = parser.getAutoTableSqlSchemaAndTable(sqlArr[2])
		return
	}

	// TRUNCATE tableName
	if strings.Index(sqlUpper, "TRUNCATE") == 0 {
		sqlArr := strings.Split(sql, " ")
		SchemaName, TableName = parser.getAutoTableSqlSchemaAndTable(sqlArr[1])
		return
	}

	// RENAME TABLE tableName
	if strings.Index(sqlUpper, "RENAME") == 0 {
		noReloadTableInfo = true
		sqlArr := strings.Split(sql, " ")
		SchemaName, TableName = parser.getAutoTableSqlSchemaAndTable(sqlArr[2])
		return
	}

	// DROP TABLE IF EXISTS tableName
	if strings.Index(sqlUpper, "DROP TABLE") == 0 {
		noReloadTableInfo = true
		sqlArr := strings.Split(sql, " ")
		var tableNameIndex = 2
		if strings.Index(sqlUpper, "IF EXISTS") > 0 {
			tableNameIndex = 4
		}
		SchemaName, TableName = parser.getAutoTableSqlSchemaAndTable(sqlArr[tableNameIndex])
		return
	}

	// CREATE TABLE IF NOT EXISTS `tableName` (
	// CREATE TABLE IF NOT EXISTS `tableName`(
	// CREATE TABLE IF `tableName`(
	if strings.Index(sqlUpper, "CREATE TABLE") == 0 {
		noReloadTableInfo = true
		sqlArr := strings.Split(sql, " ")

		// 假如 存在 IF NOT EXISTS 则代表表名是按 空格分割过后的数组里 第6个，也就是下标 5
		var tableNameIndex = 2
		if strings.Index(sqlUpper, "IF NOT EXISTS") > 0 {
			tableNameIndex = 5
		}
		//create table table(id int) 这种表名和( 相挨着的情况
		if strings.Index(sqlArr[tableNameIndex], "(") > 0 {
			tmpTableName := strings.Split(sqlArr[tableNameIndex], "(")[0]
			SchemaName, TableName = parser.getAutoTableSqlSchemaAndTable(tmpTableName)
		} else {
			SchemaName, TableName = parser.getAutoTableSqlSchemaAndTable(sqlArr[tableNameIndex])
		}
		return
	}

	// CREATE DATABASE IF NOT EXISTS databaseName
	if strings.Index(sqlUpper, "CREATE DATABASE") == 0 {
		noReloadTableInfo = true
		sqlArr := strings.Split(sql, " ")
		if strings.Index(sqlUpper, "IF NOT EXISTS") < 0 {
			SchemaName = sqlArr[2]
		} else {
			SchemaName = sqlArr[5]
		}
		return
	}

	// DROP DATABASE IF EXISTS databaseName
	if strings.Index(sqlUpper, "DROP DATABASE") == 0 {
		sqlArr := strings.Split(sql, " ")
		if strings.Index(sqlUpper, "IF EXISTS") < 0 {
			SchemaName = sqlArr[2]
		} else {
			SchemaName = sqlArr[4]
		}
		return
	}

	// CREATE INDEX index_name ON table_name (column_name)
	// CREATE UNIQUE INDEX index_name ON table_name (column_name)
	var normalIndex = strings.Index(sqlUpper, "CREATE INDEX")
	if normalIndex == 0 || strings.Index(sqlUpper, "CREATE UNIQUE INDEX") == 0 {
		sqlArr := strings.Split(sql, " ")
		var tableNameIndex = 4
		if normalIndex != 0 {
			tableNameIndex = 5
		}
		//CREATE INDEX indexName ON table(id int) 这种表名和( 相挨着的情况
		if strings.Index(sqlArr[tableNameIndex], "(") > 0 {
			tmpTableName := strings.Split(sqlArr[tableNameIndex], "(")[0]
			SchemaName, TableName = parser.getAutoTableSqlSchemaAndTable(tmpTableName)
		} else {
			SchemaName, TableName = parser.getAutoTableSqlSchemaAndTable(sqlArr[tableNameIndex])
		}
		return
	}

	// 授权
	if strings.Index(sqlUpper, "GRANT") == 0 {
		SchemaName, TableName = "", ""
		return
	}

	// ALTER USER , CREATE USER
	if strings.Index(sqlUpper, "ALTER USER") == 0 || strings.Index(sqlUpper, "CREATE USER") == 0 {
		SchemaName, TableName = "", ""
		return
	}

	noReloadTableInfo = true
	// UPDATE Table
	// INSERT INTO Table
	// DELETE FROM Table
	// REPLACE INTO Table
	var tableNameIndex = 0
	if len(sqlUpper) < 6 {
		return
	}
	switch sqlUpper[0:6] {
	case "UPDATE":
		tableNameIndex = 1
		isDDL = false
		break
	case "INSERT", "DELETE", "REPLAC":
		tableNameIndex = 2
		isDDL = false
		break
	default:
		return
	}
	if tableNameIndex == 0 {
		return
	}
	sqlArr := strings.Split(sql, " ")
	tmpTableName := sqlArr[tableNameIndex]
	if strings.Index(tmpTableName, "(") > 0 {
		tmpTableName = strings.Split(tmpTableName, "(")[0]
		SchemaName, TableName = parser.getAutoTableSqlSchemaAndTable(tmpTableName)
	} else {
		SchemaName, TableName = parser.getAutoTableSqlSchemaAndTable(tmpTableName)
	}
	return
}

// 将sql 里 /* */ 注释内容给去掉
// 感谢 @zeroone2005 正则表达式提供支持
var replaceSqlNotesReq = regexp.MustCompile(`/\*(.*?)\*/`)

func TransferNotes2Space(sql string) string {
	sql = replaceSqlNotesReq.ReplaceAllString(sql, "")
	return sql
}
