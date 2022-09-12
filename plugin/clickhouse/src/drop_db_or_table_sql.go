package src

import (
	"fmt"
	"log"
	"strings"
)

type DropDBTableOrTableSQL struct {
	DefaultSchemaName string
	Sql               string
	c                 *Conn
}

func NewDropDBOrTableSQL(DefaultSchemaName, sql string, c *Conn) *DropDBTableOrTableSQL {
	return &DropDBTableOrTableSQL{
		DefaultSchemaName: DefaultSchemaName,
		Sql:               sql,
		c:                 c,
	}
}

func (This *DropDBTableOrTableSQL) Transfer2CkSQL(c *Conn) (SchemaName, TableName, newSql, newLocalSql, newDisSql, newViewSql string) {
	var dbNameOrTableName = ""
	var disTableName = ""
	var isDatabase = false

	sql0 := strings.Split(This.Sql, " ")
	if len(sql0) < 3 {
		log.Println("invalid sql " + This.Sql)
		return
	}

	log.Println("dropDbOrTable  mysql sql: " + This.Sql)

	if strings.Trim(strings.ToUpper(sql0[1]), " ") == "DATABASE" {
		isDatabase = true
	}

	//DROP TABLE IF EXISTS `uc_department`
	if strings.Trim(strings.ToUpper(sql0[2]), " ") == "IF" &&
		strings.Trim(strings.ToUpper(sql0[3]), " ") == "EXISTS" {
		dbNameOrTableName = sql0[4]
	} else { // DROP TABLE `uc_department`
		dbNameOrTableName = sql0[2]
	}

	SchemaName, TableName = This.c.getAutoTableSqlSchemaAndTable(dbNameOrTableName, This.DefaultSchemaName)
	var tableName = TableName

	switch c.p.CkEngine {
	case 1: //单机模式
		SchemaName = This.c.GetSchemaName(SchemaName)
		TableName = This.c.GetTableName(TableName)
		if isDatabase {
			newSql = fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbNameOrTableName)
		} else {
			newSql = fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", SchemaName, TableName)
		}
		log.Println("DROP DJ CK: " + newSql)
	case 2: //集群模式
		SchemaName = This.c.GetSchemaName(SchemaName) + "_ck"
		TableName = This.c.GetTableName(TableName) + "_local"
		disTableName = This.c.GetTableName(tableName) + "_all"

		if isDatabase {
			newLocalSql = fmt.Sprintf("DROP DATABASE IF EXISTS %s ON CLUSTER %s", SchemaName, c.p.CkClusterName)
			newDisSql = fmt.Sprintf("DROP DATABASE IF EXISTS %s ON CLUSTER %s", SchemaName, c.p.CkClusterName)
			newViewSql = fmt.Sprintf("DROP DATABASE IF EXISTS %s ON CLUSTER %s", SchemaName, c.p.CkClusterName)
		} else {
			newLocalSql = fmt.Sprintf("DROP TABLE IF EXISTS %s.%s ON CLUSTER %s", SchemaName, TableName, c.p.CkClusterName)
			newDisSql = fmt.Sprintf("DROP TABLE IF EXISTS %s.%s ON CLUSTER %s", SchemaName, disTableName, c.p.CkClusterName)
			newViewSql = fmt.Sprintf("DROP TABLE IF EXISTS %s.%s ON CLUSTER %s", SchemaName, disTableName+"_pview", c.p.CkClusterName)
		}
		log.Println("DDL DROP JQ CK: " + newLocalSql + "===" + newDisSql + "===" + newViewSql)
	}
	return
}
