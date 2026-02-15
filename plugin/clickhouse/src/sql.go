package src

import (
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"strings"
)

func (This *Conn) getAutoTableSqlSchemaAndTable(name string, DefaultSchemaName string) (SchemaName, TableName string) {
	dbAndTable := strings.Replace(name, "`", "", -1)
	i := strings.IndexAny(dbAndTable, ".")
	if i > 0 {
		if This.p.CkSchema == "" {
			SchemaName = dbAndTable[0:i]
		} else {
			SchemaName = This.p.CkSchema
		}
		TableName = dbAndTable[i+1:]
	} else {
		if This.p.CkSchema == "" {
			SchemaName = DefaultSchemaName
		} else {
			SchemaName = This.p.CkSchema
		}
		TableName = dbAndTable
	}
	// 实际运行过程测试出 解析出来的 sql 中 SchemaName 和 TableName 是有换行符的,需要过滤掉，要不然拼出来的sql,会出问题
	SchemaName = ReplaceBr(SchemaName)
	TableName = ReplaceBr(TableName)
	return
}

// IsNonDDLQuery returns true if the query is a DML statement or transaction
// control statement that should NOT be processed as DDL by the ClickHouse plugin.
// This prevents data loss when MySQL tables have triggers and binlog_format is
// STATEMENT or MIXED, causing DML to be logged as QUERY_EVENT.
func IsNonDDLQuery(query string) bool {
	query = strings.TrimSpace(query)
	if len(query) < 4 {
		return false
	}
	upper := strings.ToUpper(query)
	// Transaction control statements (may appear with triggers)
	if strings.HasPrefix(upper, "SAVEPOINT") ||
		strings.HasPrefix(upper, "RELEASE SAVEPOINT") ||
		strings.HasPrefix(upper, "ROLLBACK TO") {
		return true
	}
	// DML statements from STATEMENT/MIXED binlog format
	if strings.HasPrefix(upper, "INSERT") ||
		strings.HasPrefix(upper, "UPDATE") ||
		strings.HasPrefix(upper, "DELETE") ||
		strings.HasPrefix(upper, "REPLACE") {
		return true
	}
	return false
}

func (This *Conn) TranferQuerySql(data *pluginDriver.PluginDataType) (SchemaName, TableName, newSql, newLocalSql, newDisSql, newViewSql string) {
	Query := strings.Trim(data.Query, " ")
	// 非 DDL ALTER 语句，直接过滤掉
	if len(Query) < 5 {
		return
	}
	Query = strings.Trim(strings.Trim(strings.Trim(Query, " "), ";"), " ")
	switch strings.Trim(strings.ToUpper(Query[0:5]), " ") {
	// alter
	case "ALTER":
		Query = TransferNotes2Space(Query)
		Query = ReplaceBr(Query)
		Query = ReplaceTwoReplace(Query)
		c := NewAlterSQL(data.SchemaName, Query, This)
		SchemaName, TableName, newSql, newLocalSql, newDisSql, newViewSql = c.Transfer2CkSQL(This)
		// rename
	case "RENAM":
		Query = TransferNotes2Space(Query)
		Query = ReplaceBr(Query)
		Query = ReplaceTwoReplace(Query)
		c := NewReNameSQL(data.SchemaName, Query, This)
		SchemaName, TableName, newLocalSql, newViewSql, newDisSql = c.Transfer2CkSQL(This)
	case "DROP":
		if !This.p.ModifDDLType.DropDbAndTable {
			return
		}
		Query = TransferNotes2Space(Query)
		Query = ReplaceBr(Query)
		Query = ReplaceTwoReplace(Query)
		c := NewDropDBOrTableSQL(data.SchemaName, Query, This)
		SchemaName, TableName, newSql, newLocalSql, newViewSql, newDisSql = c.Transfer2CkSQL(This)
	case "TRUNC":
		if !This.p.ModifDDLType.Rruncate {
			return
		}
		Query = TransferNotes2Space(Query)
		Query = ReplaceBr(Query)
		Query = ReplaceTwoReplace(Query)
		c := NewTruncateSQL(data.SchemaName, Query, This)
		SchemaName, TableName, newSql, newLocalSql, newViewSql, newDisSql = c.Transfer2CkSQL(This)
	default:
		break
	}
	return
}
