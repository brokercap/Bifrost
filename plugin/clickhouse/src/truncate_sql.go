package src

import (
	"fmt"
	"log"
	"strings"
)

type TruncateSQL struct {
	DefaultSchemaName string
	Sql               string
	c                 *Conn
}

func NewTruncateSQL(DefaultSchemaName, sql string, c *Conn) *TruncateSQL {
	return &TruncateSQL{
		DefaultSchemaName: DefaultSchemaName,
		Sql:               sql,
		c:                 c,
	}
}

func (This *TruncateSQL) Transfer2CkSQL(c *Conn) (SchemaName, TableName, newSql, newLocalSql, newDisSql, newViewSql string) {
	sql0 := strings.Split(This.Sql, " ")
	if len(sql0) < 3 {
		log.Println("invalid sql " + This.Sql)
		return
	}

	log.Println("truncate mysql sql: " + This.Sql)

	mysqlTableName := sql0[2]

	SchemaName, TableName = This.c.getAutoTableSqlSchemaAndTable(mysqlTableName, This.DefaultSchemaName)

	switch c.p.CkEngine {
	case 1: //单机模式
		SchemaName = This.c.GetFieldName(SchemaName)
		TableName = This.c.GetFieldName(TableName)
		newSql = fmt.Sprintf("TRUNCATE TABLE %s.%s", SchemaName, TableName)
		log.Println("Truncate DJ CK: " + newSql)
	case 2: //集群模式
		SchemaName = This.c.GetFieldName(SchemaName) + "_ck"
		TableName = This.c.GetFieldName(TableName) + "_local"

		//只需要清空每个节点的local表即可
		newLocalSql = fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s.%s ON CLUSTER %s", SchemaName, TableName, c.p.CkClusterName)
		newDisSql = fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s.%s ON CLUSTER %s", SchemaName, TableName, c.p.CkClusterName)
		newViewSql = fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s.%s ON CLUSTER %s", SchemaName, TableName, c.p.CkClusterName)
		log.Println("Truncate JQ CK: " + newLocalSql + "===" + newDisSql + "===" + newViewSql)
	}
	return
}
