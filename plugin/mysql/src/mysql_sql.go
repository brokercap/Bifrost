package src

import (
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"strings"
	"log"
)

func (This *Conn) getAutoTableSqlSchemaAndTable(name string,DefaultSchemaName string) (SchemaName,TableName string) {
	dbAndTable := strings.Replace(name, "`", "", -1)
	i := strings.IndexAny(dbAndTable, ".")
	if i > 0 {
		if This.p.Schema == "" {
			SchemaName = dbAndTable[0:i]
		}else{
			SchemaName = This.p.Schema
		}
		TableName = dbAndTable[i+1:]
	} else {
		if This.p.Schema == "" {
			SchemaName = DefaultSchemaName
		}else{
			SchemaName = This.p.Schema
		}
		TableName = dbAndTable
	}
	return
}

func (This *Conn) TranferQuerySql(data *pluginDriver.PluginDataType) (newSql string) {
	sql := strings.ToUpper(data.Query)
	var SchemaName,TableName string

	// ALTER TABLE tableName
	// ALTER TABLE 不能使用 IF EXISTS
	if strings.Index(sql,"ALTER TABLE") == 0 {
		sqlArr := strings.Split(data.Query, " ")
		SchemaName,TableName = This.getAutoTableSqlSchemaAndTable(sqlArr[2],data.SchemaName)
		sqlArr[2] = "`" + SchemaName + "`.`" + TableName +"`"
		newSql = strings.Join(sqlArr, " ")
		goto End
	}

	// TRUNCATE TABLE tableName
	if strings.Index(sql,"TRUNCATE TABLE") == 0 {
		sqlArr := strings.Split(data.Query, " ")
		var tableNameIndex = 2
		SchemaName,TableName = This.getAutoTableSqlSchemaAndTable(sqlArr[tableNameIndex],data.SchemaName)
		var schemaAndTable = "`" + SchemaName + "`.`" + TableName +"`"
		sqlArr[tableNameIndex] = schemaAndTable
		newSql = strings.Join(sqlArr, " ")
		goto End
	}

	// CREATE TABLE IF NOT EXISTS `tableName` (
	// CREATE TABLE IF NOT EXISTS `tableName`(
	// CREATE TABLE IF `tableName`(
	if strings.Index(sql,"CREATE TABLE") == 0 {
		var schemaAndTable = ""
		sqlArr := strings.Split(data.Query, " ")

		// 假如 存在 IF NOT EXISTS 则代表表名是按 空格分割过后的数组里 第6个，也就是下标 5
		var tableNameIndex = 2
		if strings.Index(sql,"IF NOT EXISTS") > 0 {
			tableNameIndex = 5
		}
		//create table table(id int) 这种表名和( 相挨着的情况
		if strings.Index(sqlArr[tableNameIndex],"(") > 0 {
			tmpTableName := strings.Split(sqlArr[tableNameIndex], "(")[0]
			SchemaName,TableName = This.getAutoTableSqlSchemaAndTable(tmpTableName,data.SchemaName)
			schemaAndTable = "`" + SchemaName + "`.`" + TableName +"`"
			// 假如不存在 IF NOT EXISTS 则给 sql 加上 IF NOT EXISTS，这里防止其他线程先执行了这句语的情况下造成的出错
			if tableNameIndex == 2 {
				schemaAndTable = " IF NOT EXISTS " + schemaAndTable
			}
			newSql = strings.Replace(data.Query,tmpTableName+"(",schemaAndTable+"(",1)
		}else{
			SchemaName,TableName = This.getAutoTableSqlSchemaAndTable(sqlArr[tableNameIndex],data.SchemaName)
			schemaAndTable = "`" + SchemaName + "`.`" + TableName +"`"
			// 假如不存在 IF NOT EXISTS 则给 sql 加上 IF NOT EXISTS，这里防止其他线程先执行了这句语的情况下造成的出错
			if tableNameIndex == 2 {
				schemaAndTable = " IF NOT EXISTS " + schemaAndTable
			}
			newSql = strings.Replace(data.Query,sqlArr[tableNameIndex],schemaAndTable,1)
		}
		goto End
	}

	// CREATE DATABASE IF NOT EXISTS databaseName
	if strings.Index(sql,"CREATE DATABASE") == 0 {
		sqlArr := strings.Split(data.Query, " ")
		if strings.Index(sql,"IF NOT EXISTS") < 0 {
			SchemaName = sqlArr[2]
		}else{
			SchemaName = sqlArr[5]
		}
		newSql = "CREATE DATABASE IF NOT EXISTS " + SchemaName + " ;"
		goto End
	}

	// RENAME TABLE
	if strings.Index(sql,"RENAME TABLE") == 0 {
		type TableInfo struct {
			From	string
			To		string
		}
		/*
		RENAME TABLE `test3` TO `test2`,`test2` TO `test4`;

		==>

		`test3` TO `test2`,`test2` TO `test4`;

		==> ["`test3` TO `test2`","`test2` TO `test4`"]

		*/
		// 这里要 trim 两次空格，防止  RENAME TABLE `test3` TO `test2`,`test2` TO `test4`   ; 这种情况
		sql0 := strings.Trim(strings.Trim(strings.Trim(data.Query[12:]," "),";")," ")
		ReNameTableArr := make([]TableInfo,0)
		sqlArr := strings.Split(sql0, ",")
		for _, reNameInfo := range sqlArr {
			FromAndToArr := strings.Split(strings.Trim(reNameInfo," "), " ")
			//`test3` TO `test2`
			FromSchemaName,FromTableName := This.getAutoTableSqlSchemaAndTable(FromAndToArr[0],data.SchemaName)
			ToSchemaName,ToTableName := This.getAutoTableSqlSchemaAndTable(FromAndToArr[2],data.SchemaName)
			TableTmp := TableInfo {
				From:"`" + FromSchemaName + "`.`" + FromTableName +"`",
				To:"`" + ToSchemaName + "`.`" + ToTableName +"`",
			}
			ReNameTableArr = append(ReNameTableArr,TableTmp)
		}
		if len(ReNameTableArr) == 0 {
			log.Println("plugin mysql rename ddl transfer err!, sql :",data.Query)
			return
		}
		for _,t := range ReNameTableArr{
			if t.From == "" || t.To == "" {
				continue
			}
			if newSql == "" {
				newSql += "RENAME TABLE " + t.From + " TO " + t.To
			}else{
				newSql += "," + t.From + " TO " + t.To
			}
		}
		goto End
	}

	// DROP TABLE IF EXISTS tableName
	if strings.Index(sql,"DROP TABLE") == 0  {
		sqlArr := strings.Split(data.Query, " ")
		var tableNameIndex = 2
		if strings.Index(sql,"IF EXISTS") > 0 {
			tableNameIndex = 4
		}
		SchemaName,TableName = This.getAutoTableSqlSchemaAndTable(sqlArr[tableNameIndex],data.SchemaName)
		var schemaAndTable = "`" + SchemaName + "`.`" + TableName +"`"
		if tableNameIndex == 2 {
			schemaAndTable = " IF EXISTS " + schemaAndTable
		}
		sqlArr[tableNameIndex] = schemaAndTable
		newSql = strings.Join(sqlArr, " ")
		goto End
	}

	// DROP DATABASE IF EXISTS databaseName
	if strings.Index(sql,"DROP DATABASE") == 0 {
		sqlArr := strings.Split(data.Query, " ")
		if strings.Index(sql,"IF EXISTS") < 0 {
			SchemaName = sqlArr[2]
		}else{
			SchemaName = sqlArr[4]
		}
		newSql = "DROP DATABASE IF EXISTS " + SchemaName + ";"
		goto End
	}else{
		// UPDATE Table
		// INSERT INTO Table
		// DELETE FROM Table
		// REPLACE INTO Table
		var tableNameIndex = 1
		switch sql[0:6] {
		case "UPDATE":
			break
		case "INSERT","DELETE","REPLAC":
			tableNameIndex = 2
			break
		default:
			return
		}
		sqlArr := strings.Split(data.Query, " ")
		tmpTableName := sqlArr[tableNameIndex]
		var schemaAndTable string
		if strings.Index(tmpTableName,"(") > 0 {
			tmpTableName = strings.Split(tmpTableName,"(")[0]
			SchemaName,TableName = This.getAutoTableSqlSchemaAndTable(tmpTableName,data.SchemaName)
			schemaAndTable = "`" + SchemaName + "`.`" + TableName +"`"
			newSql = strings.Replace(data.Query,tmpTableName+"(",schemaAndTable+"(",1)
		}else{
			SchemaName,TableName = This.getAutoTableSqlSchemaAndTable(tmpTableName,data.SchemaName)
			schemaAndTable = "`" + SchemaName + "`.`" + TableName +"`"
			newSql = strings.Replace(data.Query,tmpTableName,schemaAndTable,1)
		}
	}

	End:
		return
}

