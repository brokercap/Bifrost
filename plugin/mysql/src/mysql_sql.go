package src

import (
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"log"
	"regexp"
	"strings"
)

func (This *Conn) getAutoTableSqlSchemaAndTable(name string, DefaultSchemaName string) (SchemaName, TableName string) {
	dbAndTable := strings.Replace(name, "`", "", -1)
	i := strings.IndexAny(dbAndTable, ".")
	if i > 0 {
		if This.p.Schema == "" {
			SchemaName = dbAndTable[0:i]
		} else {
			SchemaName = This.p.Schema
		}
		TableName = dbAndTable[i+1:]
	} else {
		if This.p.Schema == "" {
			SchemaName = DefaultSchemaName
		} else {
			SchemaName = This.p.Schema
		}
		TableName = dbAndTable
	}
	// 实际运行过程测试出 解析出来的 sql 中 SchemaName 和 TableName 是有换行符的,需要过滤掉，要不然拼出来的sql,会出问题
	SchemaName = strings.Trim(SchemaName, "\r\n")
	SchemaName = strings.Trim(SchemaName, "\n")
	SchemaName = strings.Trim(SchemaName, "\r")
	TableName = strings.Trim(TableName, "\r\n")
	TableName = strings.Trim(TableName, "\n")
	TableName = strings.Trim(TableName, "\r")
	return
}

// 将sql 里 /* */ 注释内容给去掉
// 感谢 @zeroone2005 正则表达式提供支持
var replaceSqlNotesReq = regexp.MustCompile(`/\*(.*?)\*/`)

func (This *Conn) TransferNotes2Space(sql string) string {
	sql = replaceSqlNotesReq.ReplaceAllString(sql, "")
	return sql
}

// 去除连续的两个空格
func (This *Conn) ReplaceTwoReplace(sql string) string {
	for {
		if strings.Index(sql, "  ") >= 0 {
			sql = strings.Replace(sql, "  ", " ", -1)
		} else {
			return sql
		}
	}
}

func (This *Conn) TranferQuerySql(data *pluginDriver.PluginDataType) (newSqlArr []string) {
	// 优先判断是否 DML 语句
	newSqlArr = This.TranferDMLSql(data)
	if len(newSqlArr) > 0 {
		return
	}
	var newSql string
	var Query = strings.Trim(data.Query, " ")
	Query = This.TransferNotes2Space(Query)
	// 变量 sql 是就不用拼接最后的  可执行 sql的，所以可以全部转成大写
	sql := strings.ToUpper(Query)
	// 防止连续多空格
	// RENAME      TABLE tablename to tablename2
	// Create   Table
	// create      database
	sql = This.ReplaceTwoReplace(sql)
	var SchemaName, TableName string

	// ALTER TABLE tableName
	// ALTER TABLE 不能使用 IF EXISTS
	if strings.Index(sql, "ALTER TABLE") == 0 {
		Query = This.ReplaceTwoReplace(Query)
		sqlArr := strings.Split(Query, " ")
		SchemaName, TableName = This.getAutoTableSqlSchemaAndTable(sqlArr[2], data.SchemaName)
		sqlArr[2] = "`" + SchemaName + "`.`" + TableName + "`"
		newSql = strings.Join(sqlArr, " ")
		newSqlArr = append(newSqlArr, newSql)
		goto End
	}

	// TRUNCATE TABLE tableName
	if strings.Index(sql, "TRUNCATE") == 0 {
		Query = This.ReplaceTwoReplace(Query)
		sqlArr := strings.Split(Query, " ")
		var tableNameIndex int
		if strings.Index(sql, "TRUNCATE TABLE ") == 0 {
			tableNameIndex = 2
		} else {
			tableNameIndex = 1
		}
		SchemaName, TableName = This.getAutoTableSqlSchemaAndTable(sqlArr[tableNameIndex], data.SchemaName)
		var schemaAndTable = "`" + SchemaName + "`.`" + TableName + "`"
		sqlArr[tableNameIndex] = schemaAndTable
		newSql = strings.Join(sqlArr, " ")
		newSqlArr = append(newSqlArr, newSql)
		goto End
	}

	// CREATE TABLE IF NOT EXISTS `tableName` (
	// CREATE TABLE IF NOT EXISTS `tableName`(
	// CREATE TABLE IF `tableName`(
	if strings.Index(sql, "CREATE TABLE") == 0 {
		var schemaAndTable = ""
		Query = This.ReplaceTwoReplace(Query)
		sqlArr := strings.Split(Query, " ")

		// 假如 存在 IF NOT EXISTS 则代表表名是按 空格分割过后的数组里 第6个，也就是下标 5
		var tableNameIndex = 2
		if strings.Index(sql, "IF NOT EXISTS") > 0 {
			tableNameIndex = 5
		}
		//create table table(id int) 这种表名和( 相挨着的情况
		if strings.Index(sqlArr[tableNameIndex], "(") > 0 {
			tmpTableName := strings.Split(sqlArr[tableNameIndex], "(")[0]
			SchemaName, TableName = This.getAutoTableSqlSchemaAndTable(tmpTableName, data.SchemaName)
			schemaAndTable = "`" + SchemaName + "`.`" + TableName + "`"
			// 假如不存在 IF NOT EXISTS 则给 sql 加上 IF NOT EXISTS，这里防止其他线程先执行了这句语的情况下造成的出错
			if tableNameIndex == 2 {
				schemaAndTable = " IF NOT EXISTS " + schemaAndTable
			}
			newSql = strings.Replace(Query, tmpTableName+"(", schemaAndTable+"(", 1)
		} else {
			SchemaName, TableName = This.getAutoTableSqlSchemaAndTable(sqlArr[tableNameIndex], data.SchemaName)
			schemaAndTable = "`" + SchemaName + "`.`" + TableName + "`"
			// 假如不存在 IF NOT EXISTS 则给 sql 加上 IF NOT EXISTS，这里防止其他线程先执行了这句语的情况下造成的出错
			if tableNameIndex == 2 {
				schemaAndTable = " IF NOT EXISTS " + schemaAndTable
			}
			newSql = strings.Replace(Query, sqlArr[tableNameIndex], schemaAndTable, 1)
		}
		newSqlArr = append(newSqlArr, newSql)
		goto End
	}

	// CREATE DATABASE IF NOT EXISTS databaseName
	if strings.Index(sql, "CREATE DATABASE") == 0 {
		Query = This.ReplaceTwoReplace(Query)
		sqlArr := strings.Split(Query, " ")
		if strings.Index(sql, "IF NOT EXISTS") < 0 {
			sqlArr[1] = "DATABASE IF NOT EXISTS"
			newSql = strings.Join(sqlArr, " ")
		} else {
			newSql = Query
		}
		newSqlArr = append(newSqlArr, newSql)
		goto End
	}

	// RENAME TABLE
	if strings.Index(sql, "RENAME TABLE") == 0 {
		type TableInfo struct {
			From string
			To   string
		}
		/*
			RENAME TABLE `test3` TO `test2`,`test2` TO `test4`;

			==>

			`test3` TO `test2`,`test2` TO `test4`;

			==> ["`test3` TO `test2`","`test2` TO `test4`"]

		*/
		// 这里要 trim 两次空格，防止  RENAME TABLE `test3` TO `test2`,`test2` TO `test4`   ; 这种情况
		Query = This.ReplaceTwoReplace(Query)
		sql0 := strings.Trim(strings.Trim(strings.Trim(Query[12:], " "), ";"), " ")
		ReNameTableArr := make([]TableInfo, 0)
		sqlArr := strings.Split(sql0, ",")
		for _, reNameInfo := range sqlArr {
			FromAndToArr := strings.Split(strings.Trim(reNameInfo, " "), " ")
			//`test3` TO `test2`
			FromSchemaName, FromTableName := This.getAutoTableSqlSchemaAndTable(FromAndToArr[0], data.SchemaName)
			ToSchemaName, ToTableName := This.getAutoTableSqlSchemaAndTable(FromAndToArr[2], data.SchemaName)
			TableTmp := TableInfo{
				From: "`" + FromSchemaName + "`.`" + FromTableName + "`",
				To:   "`" + ToSchemaName + "`.`" + ToTableName + "`",
			}
			ReNameTableArr = append(ReNameTableArr, TableTmp)
		}
		if len(ReNameTableArr) == 0 {
			log.Println("plugin mysql rename ddl transfer err!, sql :", Query)
			return
		}
		for _, t := range ReNameTableArr {
			if t.From == "" || t.To == "" {
				continue
			}
			// TiDB 不支持  一条语句，多次 rename , 所以要分成多个rename 语句
			if This.isTiDB {
				newSql = "RENAME TABLE " + t.From + " TO " + t.To
				newSqlArr = append(newSqlArr, newSql)
			} else {
				if newSql == "" {
					newSql = "RENAME TABLE " + t.From + " TO " + t.To
				} else {
					newSql += "," + t.From + " TO " + t.To
				}
			}
		}
		if This.isTiDB == false && newSql != "" {
			newSqlArr = append(newSqlArr, newSql)
		}
		goto End
	}

	// DROP TABLE IF EXISTS tableName
	if strings.Index(sql, "DROP TABLE") == 0 {
		Query = This.ReplaceTwoReplace(Query)
		sqlArr := strings.Split(Query, " ")
		var tableNameIndex = 2
		if strings.Index(sql, "IF EXISTS") > 0 {
			tableNameIndex = 4
		}
		SchemaName, TableName = This.getAutoTableSqlSchemaAndTable(sqlArr[tableNameIndex], data.SchemaName)
		var schemaAndTable = "`" + SchemaName + "`.`" + TableName + "`"
		if tableNameIndex == 2 {
			schemaAndTable = " IF EXISTS " + schemaAndTable
		}
		sqlArr[tableNameIndex] = schemaAndTable
		newSql = strings.Join(sqlArr, " ")
		newSqlArr = append(newSqlArr, newSql)
		goto End
	}

	// CREATE INDEX index_name ON table_name (column_name)
	// CREATE UNIQUE INDEX index_name ON table_name (column_name)
	if strings.Index(sql, "CREATE INDEX") == 0 || strings.Index(sql, "CREATE UNIQUE INDEX") == 0 {
		Query = This.ReplaceTwoReplace(Query)
		sqlArr := strings.Split(Query, " ")
		var tableNameIndex = 4
		if strings.Index(sql, "CREATE INDEX") != 0 {
			tableNameIndex = 5
		}
		var schemaAndTable string
		//CREATE INDEX indexName ON table(id int) 这种表名和( 相挨着的情况
		if strings.Index(sqlArr[tableNameIndex], "(") > 0 {
			tmpTableName := strings.Split(sqlArr[tableNameIndex], "(")[0]
			SchemaName, TableName = This.getAutoTableSqlSchemaAndTable(tmpTableName, data.SchemaName)
			schemaAndTable = "`" + SchemaName + "`.`" + TableName + "`"
			newSql = strings.Replace(Query, tmpTableName+"(", schemaAndTable+"(", 1)
		} else {
			SchemaName, TableName = This.getAutoTableSqlSchemaAndTable(sqlArr[tableNameIndex], data.SchemaName)
			schemaAndTable = "`" + SchemaName + "`.`" + TableName + "`"
			newSql = strings.Replace(Query, sqlArr[tableNameIndex], schemaAndTable, 1)
		}
		newSqlArr = append(newSqlArr, newSql)
		goto End
	}

	// DROP DATABASE IF EXISTS databaseName
	if strings.Index(sql, "DROP DATABASE") == 0 {
		Query = This.ReplaceTwoReplace(Query)
		sqlArr := strings.Split(Query, " ")
		if strings.Index(sql, "IF EXISTS") < 0 {
			SchemaName = sqlArr[2]
		} else {
			SchemaName = sqlArr[4]
		}
		newSql = "DROP DATABASE IF EXISTS " + SchemaName + ";"
		newSqlArr = append(newSqlArr, newSql)
		goto End
	}
End:
	return
}

func (This *Conn) TranferDMLSql(data *pluginDriver.PluginDataType) (newSqlArr []string) {
	var Query = strings.TrimLeft(data.Query, " ")
	var SchemaName, TableName string
	// UPDATE Table
	// INSERT INTO Table
	// DELETE FROM Table
	// REPLACE INTO Table
	var tableNameIndex = 0
	// insert,update,replace 字符串后第几个非空的字符串，才是第表名
	var x = 1
	switch strings.ToUpper(Query[0:6]) {
	case "UPDATE":
		break
	case "INSERT", "DELETE", "REPLAC":
		x = 2
		break
	default:
		return
	}
	// 这里不能使用 ReplaceTwoReplace 将  两个空格转成一个空格再进行计算 ，因为实际 insert 或者 update 等的内容里是值有可能是 两个空格的内容
	// 这里也不能用 TransferNotes2Space , 因为实际insert ,upadte 语句中，就包括 /* */ 内容
	// 这里采用 遍历 的方式，找第一个或者第二个非空的字段串，当作是表名
	sqlArr := strings.Split(Query, " ")
	var tmpX = 0
	var inNotes = false
	for i := 1; i < len(sqlArr); i++ {
		var tmp = strings.Trim(sqlArr[i], " ")
		if tmp == "" || tmp == " " {
			continue
		}
		if strings.Index(tmp, "/*") >= 0 {
			inNotes = true
		}
		if inNotes {
			if strings.Index(tmp, "*/") >= 0 {
				inNotes = false
			}
			continue
		}
		tmpX++
		if tmpX == x {
			tableNameIndex = i
			break
		}
	}
	tmpTableName := sqlArr[tableNameIndex]
	var schemaAndTable string
	var newSql string
	if strings.Index(tmpTableName, "(") > 0 {
		tmpTableName = strings.Split(tmpTableName, "(")[0]
		SchemaName, TableName = This.getAutoTableSqlSchemaAndTable(tmpTableName, data.SchemaName)
		schemaAndTable = "`" + SchemaName + "`.`" + TableName + "`"
		newSql = strings.Replace(Query, tmpTableName+"(", schemaAndTable+"(", 1)
	} else {
		SchemaName, TableName = This.getAutoTableSqlSchemaAndTable(tmpTableName, data.SchemaName)
		schemaAndTable = "`" + SchemaName + "`.`" + TableName + "`"
		newSql = strings.Replace(Query, tmpTableName, schemaAndTable, 1)
	}
	newSqlArr = append(newSqlArr, newSql)
	return
}
