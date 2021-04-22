package src

import (
	"strings"
)


/*
  ALTER TABLE TableName
  DROP COLUMN `name`,
  CHANGE `number` `number` BIGINT(20) NOT NULL  COMMENT '馆藏数量',
  ADD COLUMN `f1` VARCHAR(200) NULL AFTER `number`,
  ADD INDEX `sdfsdfsdf` (`number`);
*/

type ReNameSQL struct {
	DefaultSchemaName string
	Sql string
	c	*Conn
}

func NewReNameSQL(DefaultSchemaName,sql string,c *Conn) *ReNameSQL {
	return &ReNameSQL{
		DefaultSchemaName:DefaultSchemaName,
		Sql:sql,
		c:c,
	}
}

func (This *ReNameSQL) Transfer2CkSQL() (SchemaName,TableName,destAlterSql string) {
	// RENAME TABLE
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
	sql0 := strings.Trim(strings.Trim(strings.Trim(This.Sql[12:]," "),";")," ")
	ReNameTableArr := make([]TableInfo,0)
	sqlArr := strings.Split(sql0, ",")
	for i, reNameInfo := range sqlArr {
		FromAndToArr := strings.Split(strings.Trim(reNameInfo," "), " ")
		//`test3` TO `test2`
		FromSchemaName,FromTableName := This.c.getAutoTableSqlSchemaAndTable(FromAndToArr[0],This.DefaultSchemaName)
		ToSchemaName,ToTableName := This.c.getAutoTableSqlSchemaAndTable(FromAndToArr[2],This.DefaultSchemaName)

		FromSchemaName = This.c.GetFieldName(FromSchemaName)
		FromTableName = This.c.GetFieldName(FromTableName)

		ToSchemaName = This.c.GetFieldName(ToSchemaName)
		ToTableName = This.c.GetFieldName(ToTableName)
		TableTmp := TableInfo {
			From:"`" + FromSchemaName + "`.`" + FromTableName +"`",
			To:"`" + ToSchemaName + "`.`" + ToTableName +"`",
		}
		ReNameTableArr = append(ReNameTableArr,TableTmp)
		if i == 0 {
			SchemaName = FromSchemaName
			TableName = FromTableName
		}
	}
	if len(ReNameTableArr) == 0 {
		return
	}
	for _,t := range ReNameTableArr{
		if t.From == "" || t.To == "" {
			continue
		}
		if destAlterSql == "" {
			destAlterSql += "RENAME TABLE " + t.From + " TO " + t.To
		}else{
			destAlterSql += "," + t.From + " TO " + t.To
		}
	}
	return
}

