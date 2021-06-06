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
	Sql               string
	c                 *Conn
}

func NewReNameSQL(DefaultSchemaName, sql string, c *Conn) *ReNameSQL {
	return &ReNameSQL{
		DefaultSchemaName: DefaultSchemaName,
		Sql:               sql,
		c:                 c,
	}
}

func (This *ReNameSQL) Transfer2CkSQL(c *Conn) (SchemaName, TableName, destAlterSql, destAlterViewSql, destAlterDisSql string) {
	// RENAME TABLE
	type TableInfo struct {
		From    string
		DisFrom string
		To      string
		DisTo   string
	}
	/*
		RENAME TABLE `test3` TO `test2`,`test2` TO `test4`;

		==>

		`test3` TO `test2`,`test2` TO `test4`;

		==> ["`test3` TO `test2`","`test2` TO `test4`"]

	*/
	sql0 := strings.Trim(strings.Trim(strings.Trim(This.Sql[12:], " "), ";"), " ")
	ReNameTableArr := make([]TableInfo, 0)
	sqlArr := strings.Split(sql0, ",")
	for i, reNameInfo := range sqlArr {
		FromAndToArr := strings.Split(strings.Trim(reNameInfo, " "), " ")
		//`test3` TO `test2`
		FromSchemaName, FromTableName := This.c.getAutoTableSqlSchemaAndTable(FromAndToArr[0], This.DefaultSchemaName)
		ToSchemaName, ToTableName := This.c.getAutoTableSqlSchemaAndTable(FromAndToArr[2], This.DefaultSchemaName)

		var TableTmp = TableInfo{}

		switch c.p.CkEngine {
		case 1: //单机
			if !c.p.ModifDDLType.TableRename {
				return
			}
			FromSchemaName = This.c.GetFieldName(FromSchemaName)
			FromTableName = This.c.GetFieldName(FromTableName)

			ToSchemaName = This.c.GetFieldName(ToSchemaName)
			ToTableName = This.c.GetFieldName(ToTableName)

			TableTmp = TableInfo{
				From: FromSchemaName + "." + FromTableName,
				To:   ToSchemaName + "." + ToTableName,
			}
		case 2: //集群
			/*var DisFromTableName = This.c.GetFieldName(FromTableName)
			var DisToTableName = This.c.GetFieldName(ToTableName)

			FromSchemaName = This.c.GetFieldName(FromSchemaName) + "_ck"
			FromTableName = This.c.GetFieldName(FromTableName) + "_local"
			DisFromTableName = This.c.GetFieldName(DisFromTableName) + "_all"

			ToSchemaName = This.c.GetFieldName(ToSchemaName) + "_ck"
			ToTableName = This.c.GetFieldName(ToTableName) + "_local"
			DisToTableName = This.c.GetFieldName(DisToTableName) + "_all"

			TableTmp = TableInfo{
				From:    FromSchemaName + "." + FromTableName,
				DisFrom: FromSchemaName + "." + DisFromTableName,
				To:      ToSchemaName + "." + ToTableName,
				DisTo:   ToSchemaName + "." + DisToTableName,
			}*/
			//集群模式根据业务需求暂时不需要rename 集群模式牵扯local表和分布式表  目前只想到了分布式表删除重建模式，可能会对后期业务有影响
			return
		}

		ReNameTableArr = append(ReNameTableArr, TableTmp)
		if i == 0 {
			SchemaName = FromSchemaName
			TableName = FromTableName
		}
	}
	if len(ReNameTableArr) == 0 {
		return
	}
	for _, t := range ReNameTableArr {
		switch c.p.CkEngine {
		case 1: //单节点
			if t.From == "" || t.To == "" {
				continue
			}
			if destAlterSql == "" {
				destAlterSql += "RENAME TABLE " + t.From + " TO " + t.To
			} else {
				destAlterSql += "," + t.From + " TO " + t.To
			}
		case 2: //集群
			if t.From == "" || t.To == "" || t.DisFrom == "" || t.DisTo == "" {
				continue
			}
			if destAlterDisSql == "" {
				//destAlterLocalSql += "RENAME TABLE " + t.From + " TO " + t.To
				destAlterSql += "RENAME TABLE " + t.From + " TO " + t.To
				destAlterDisSql += "RENAME TABLE " + t.DisFrom + " TO " + t.DisTo
				destAlterViewSql += "RENAME TABLE " + t.DisFrom + "_pview" + " TO " + t.DisTo + "_pview"
			} else {
				destAlterSql += "RENAME TABLE " + t.From + " TO " + t.To
				destAlterDisSql += "RENAME TABLE " + t.DisFrom + "_all" + " TO " + t.DisTo + "_all"
				destAlterViewSql += "," + t.DisFrom + "_pview" + " TO " + t.DisTo + "_pview"
			}
		}

	}

	//分布式操作
	if c.p.CkEngine == 2 {
		if c.p.CkClusterName == "" {
			return
		}
		//destAlterSql += " on cluster " + c.p.CkClusterName
		destAlterDisSql += " on cluster " + c.p.CkClusterName
		destAlterViewSql += " on cluster " + c.p.CkClusterName
	}
	return
}
