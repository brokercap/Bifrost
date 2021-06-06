package src

import (
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
)

/*
  ALTER TABLE TableName
  DROP COLUMN `name`,
  CHANGE `number` `number` BIGINT(20) NOT NULL  COMMENT '馆藏数量',
  ADD COLUMN `f1` VARCHAR(200) NULL AFTER `number`,
  ADD INDEX `sdfsdfsdf` (`number`);
*/

type AlterSQL struct {
	DefaultSchemaName string
	Sql               string
	c                 *Conn
}

type AlterColumnInfo struct {
	isUnsigned bool
	AfterName  string
	Default    *string
	Nullable   bool
	Comment    string
}

// 将sql 里 (,) 和  单引号，双引号 里包括的 逗号 先替换成  #@%
// 感谢 @zeroone2005 正则表达式提供支持
func TransferComma2Other(sql string) string {
	re := regexp.MustCompile(`\((\d+?),(\d+?)\)`)
	sql = re.ReplaceAllString(sql, "(${1}#@%${2})")
	re = regexp.MustCompile(`'(.*?),(.*?)'`)
	sql = re.ReplaceAllString(sql, "'$1#@%$2'")
	re = regexp.MustCompile(`"(.*?),(.*?)"`)
	sql = re.ReplaceAllString(sql, "\"$1#@%$2\"")
	return sql
}

// 将 #@% 再替换回原来的 逗号
func TransferOther2Comma(str string) string {
	str = strings.ReplaceAll(str, "#@%", ",")
	return str
}

func NewAlterSQL(DefaultSchemaName, sql string, c *Conn) *AlterSQL {
	return &AlterSQL{
		DefaultSchemaName: DefaultSchemaName,
		Sql:               sql,
		c:                 c,
	}
}

func (This *AlterSQL) Transfer2CkSQL(c *Conn) (SchemaName, TableName, destAlterSql, destLocalAlterSql, destDisAlterSql, destViewAlterSql string) {
	var disTableName = ""

	sql0 := TransferComma2Other(This.Sql)
	var sql0Arr = strings.Split(sql0, ",")
	if len(sql0Arr) <= 1 {
		sql0Arr = strings.Split(sql0, "#@%")
	}

	alterParamArr := make([]string, 0)
	for i, v := range sql0Arr {
		v = ReplaceBr(v)
		v = strings.Trim(v, " ")
		v = TransferOther2Comma(v)
		UpperV := strings.ToUpper(v)
		log.Println("DDL ALTER: " + UpperV)
		// 假如是第一个，则要去除  ALTER TABLE tableName
		if i == 0 && strings.Index(UpperV, "ALTER TABLE") == 0 {
			tmpArr := strings.Split(v, " ")
			SchemaName, TableName = This.c.getAutoTableSqlSchemaAndTable(tmpArr[2], This.DefaultSchemaName)

			var tableName = TableName
			switch c.p.CkEngine {
			case 1: //单机模式
				SchemaName = This.c.GetFieldName(SchemaName)
				TableName = This.c.GetFieldName(TableName)
			case 2: //集群模式
				SchemaName = This.c.GetFieldName(SchemaName) + "_ck"
				TableName = This.c.GetFieldName(TableName) + "_local"
				disTableName = This.c.GetFieldName(tableName) + "_all"
			}

			v = strings.Join(tmpArr[3:], " ")
			v = strings.Trim(v, " ")
			UpperV = strings.ToUpper(v)
		}
		if c.p.ModifDDLType.ColumnModify && strings.Index(UpperV, "CHANGE") == 0 {
			columnChange := This.ChangeColumn(v)
			if columnChange == "" {
				continue
			}
			alterParamArr = append(alterParamArr, columnChange)
			continue
		}
		if c.p.ModifDDLType.ColumnAdd && strings.Index(UpperV, "ADD") == 0 {
			columnAdd := This.AddColumn(v)
			if columnAdd == "" {
				continue
			}
			alterParamArr = append(alterParamArr, columnAdd)
			continue
		}
		if c.p.ModifDDLType.ColumnModify && strings.Index(UpperV, "MODIFY") == 0 {
			columnModify := This.ModifyColumn(v)
			if columnModify == "" {
				continue
			}
			alterParamArr = append(alterParamArr, columnModify)
			continue
		}
		if c.p.ModifDDLType.ColumnDrop && strings.Index(UpperV, "DROP COLUMN") == 0 {
			columnDrop := This.DropColumn(v)
			if columnDrop == "" {
				continue
			}
			alterParamArr = append(alterParamArr, columnDrop)
			continue
		}
		/*
			if strings.Index(UpperV,"ADD INDEX") == 0 {
				continue
			}
			if strings.Index(UpperV,"DROP PRIMARY") == 0 {
				continue
			}
			if strings.Index(UpperV,"ADD PRIMARY") == 0 {
				continue
			}
			if strings.Index(UpperV,"ADD FOREIGN KEY") == 0 {
				continue
			}
			if strings.Index(UpperV,"DROP FOREIGN KEY") == 0 {
				continue
			}
		*/
	}
	if len(alterParamArr) == 0 {
		return
	}

	switch c.p.CkEngine {
	case 1: //单机模式
		//单机下的最终ddl语句
		destAlterSql = "alter table `" + SchemaName + "`.`" + TableName + "` " + strings.Join(alterParamArr, ",")
		log.Println("DDL ALTER DJ CK: " + destAlterSql)
	case 2: //集群模式
		//集群下的本地表和分布式表最终的 ddl 语句
		if c.p.CkClusterName == "" {
			return
		}
		destLocalAlterSql = "alter table `" + SchemaName + "`.`" + TableName + "`  on cluster " + c.p.CkClusterName + " " + strings.Join(alterParamArr, ",")
		destDisAlterSql = "alter table `" + SchemaName + "`.`" + disTableName + "`  on cluster " + c.p.CkClusterName + " " + strings.Join(alterParamArr, ",")
		destViewAlterSql = fmt.Sprintf("Drop TABLE IF EXISTS %s.%s on cluster %s;create view IF NOT EXISTS %s.%s on cluster %s as "+
			"select * from %s.%s final",
			SchemaName, disTableName+"_"+"pview", c.p.CkClusterName, SchemaName, disTableName+"_"+"pview", c.p.CkClusterName, SchemaName, disTableName)

		log.Println("DDL ALTER JQ CK: " + destLocalAlterSql + "===" + destDisAlterSql + "===" + destViewAlterSql)
	}

	return
}

func (This *AlterSQL) DropColumn(sql string) (destAlterSql string) {
	pArr := strings.Split(sql, " ")
	destAlterSql += " DROP COLUMN IF EXISTS " + pArr[2] + ""
	return
}

/*
mysql : CHANGE `number` `number` BIGINT(20) NOT NULL  COMMENT '馆藏数量',
ck : modify column column_name [type] [default_expr]
*/
func (This *AlterSQL) ChangeColumn(sql string) (destAlterSql string) {
	var columnName, ckType string
	pArr := strings.Split(sql, " ")
	//经测试 不同mysql客户端可视化操作 生成的sql有差异 比如： sqlyog[change 不带column]  Navicat[change 带column]
	if strings.ToUpper(pArr[0]) == "CHANGE" && strings.ToUpper(pArr[1]) == "COLUMN" {
		if pArr[2] != pArr[3] {
			destAlterSql += " RENAME COLUMN  IF EXISTS " + pArr[2] + "  TO " + pArr[3] + ""
			return
		}
	} else {
		if pArr[1] != pArr[2] {
			destAlterSql += " RENAME COLUMN  IF EXISTS " + pArr[1] + "  TO " + pArr[2] + ""
			return
		}
	}

	columnName = pArr[2]
	ckType = This.GetTransferCkType(pArr[3])
	var AlterColumn = &AlterColumnInfo{}
	if len(pArr) > 4 {
		AlterColumn = This.GetColumnInfo(pArr[4:])
	}

	if AlterColumn.isUnsigned {
		// mysql 里，float double ,decimal 是可以设置 unsigned
		switch ckType {
		case "Float32", "Float64", "String":
			break
		default:
			ckType = "U" + ckType
		}
	}
	if AlterColumn.Nullable == true {
		ckType = " Nullable(" + ckType + ")"
	}
	destAlterSql = "MODIFY COLUMN IF EXISTS " + columnName + " " + ckType
	if AlterColumn.Comment != "" {
		destAlterSql += " COMMENT " + AlterColumn.Comment + ""
	}
	return
}

/*
mysql : MODIFY column `number` BIGINT(20) NOT NULL  COMMENT '馆藏数量',
ck : modify column column_name [type] [default_expr]
*/
func (This *AlterSQL) ModifyColumn(sql string) (destAlterSql string) {
	var columnName, ckType string
	pArr := strings.Split(sql, " ")

	var AlterColumn = &AlterColumnInfo{}
	if strings.ToUpper(pArr[0]) == "MODIFY" && strings.ToUpper(pArr[1]) == "COLUMN" {
		columnName = pArr[2]
		ckType = This.GetTransferCkType(pArr[3])
		if len(pArr) > 4 {
			AlterColumn = This.GetColumnInfo(pArr[4:])
		}
	} else {
		columnName = pArr[1]
		ckType = This.GetTransferCkType(pArr[2])
		if len(pArr) > 3 {
			AlterColumn = This.GetColumnInfo(pArr[3:])
		}
	}

	if AlterColumn.isUnsigned {
		// mysql 里，float double ,decimal 是可以设置 unsigned
		switch ckType {
		case "Float32", "Float64", "String":
			break
		default:
			ckType = "U" + ckType
		}
	}
	if AlterColumn.Nullable == true {
		ckType = " Nullable(" + ckType + ")"
	}
	destAlterSql = "MODIFY COLUMN IF EXISTS " + columnName + " " + ckType
	if AlterColumn.Comment != "" {
		destAlterSql += " COMMENT " + AlterColumn.Comment + ""
	}
	return
}

/*
mysql : ADD COLUMN `f1` VARCHAR(200) NULL AFTER `number`,
ck : add column column_name [type] [default_expr] [after name_after]
*/
func (This *AlterSQL) AddColumn(sql string) (destAlterSql string) {
	var columnNameIndex = 1
	if strings.Index(strings.ToUpper(sql), "ADD PRIMARY") == 0 || strings.Index(strings.ToUpper(sql), "ADD INDEX") == 0 || strings.Index(strings.ToUpper(sql), "ADD FOREIGN KEY") == 0 { //添加主键操作 ck不支持 直接过滤
		return
	}
	if strings.Index(strings.ToUpper(sql), "ADD COLUMN") == 0 {
		columnNameIndex = 2
	}
	var columnName, ckType string
	pArr := strings.Split(sql, " ")
	if len(pArr) <= columnNameIndex {
		return
	}

	columnName = pArr[columnNameIndex]
	if columnName == "" {
		return
	}
	ckType = This.GetTransferCkType(pArr[columnNameIndex+1])
	var AlterColumn = &AlterColumnInfo{}
	var columnOtherInfoIndex = columnNameIndex + 2
	if len(pArr) > columnOtherInfoIndex {
		AlterColumn = This.GetColumnInfo(pArr[columnOtherInfoIndex:])
	}

	if AlterColumn.isUnsigned {
		// mysql 里，float double ,decimal 是可以设置 unsigned
		switch ckType {
		case "Float32", "Float64", "String":
			break
		default:
			ckType = "U" + ckType
		}
	}
	if AlterColumn.Nullable == true {
		ckType = " Nullable(" + ckType + ")"
	}

	destAlterSql = "add column IF NOT EXISTS " + columnName + " " + ckType
	if AlterColumn.Comment != "" {
		destAlterSql += " COMMENT " + AlterColumn.Comment + ""
	}
	return
}

func (This *AlterSQL) GetColumnInfo(pArr []string) *AlterColumnInfo {
	AlterColumn := &AlterColumnInfo{Nullable: false}
	var key string
	var val string
	var valFirst string
	for _, v := range pArr {
		UpperV := strings.ToUpper(v)
		if UpperV == "UNSIGNED" {
			AlterColumn.isUnsigned = true
			key, val, valFirst = "", "", ""
			continue
		}
		if key == "" {
			if UpperV == "NULL" {
				AlterColumn.Nullable = true
				key, val, valFirst = "", "", ""
				continue
			}
			key = UpperV
			continue
		}
		if valFirst == "" {
			valFirst = v[0:1]
		}
		var last string
		if valFirst != "" {
			n := len(v)
			if n > 0 {
				last = v[n-1 : n]
			}
		}
		switch valFirst {
		case "'", "\"":
			val += " " + v
			if last != valFirst {
				continue
			}
		default:
			val += v
			break
		}

		switch key {
		case "DEFAULT":
			if strings.ToUpper(val) == "NULL" {
				key, val, valFirst = "", "", ""
				break
			}
			val0 := val
			AlterColumn.Default = &val0
		case "COMMENT":
			AlterColumn.Comment = val
		case "AFTER":
			AlterColumn.AfterName = val
			key, val = "", ""
		case "NOT":
			if UpperV == "NULL" {
				AlterColumn.Nullable = false
			}
			break
		default:
			break
		}
		key, val, valFirst = "", "", ""
	}
	return AlterColumn
}

func (This *AlterSQL) GetTransferCkType(mysqlColumnType string) (ckType string) {
	var mysqlDataType string
	var dataTypeParam string
	n := strings.Index(mysqlColumnType, "(")
	if n > 0 {
		mysqlDataType = strings.ToLower(mysqlColumnType[0:n])
		dataTypeParam = mysqlColumnType[n+1 : len(mysqlColumnType)-1]
		dataTypeParam = strings.Trim(dataTypeParam, " ")
	} else {
		mysqlDataType = strings.ToLower(mysqlColumnType)
	}
	switch mysqlDataType {
	case "tinyint":
		ckType = "Int8"
	case "smallint", "year":
		ckType = "Int16"
	case "mediumint", "int":
		ckType = "Int32"
	case "bigint":
		ckType = "Int64"
	case "numeric", "decimal":
		if dataTypeParam == "" {
			ckType = "Decimal(18,2)"
		} else {
			p := strings.Split(dataTypeParam, ",")
			M, _ := strconv.Atoi(strings.Trim(p[0], " "))
			// M,D.   M > 18 就属于 Decimal128 , M > 39 就属于 Decimal256  ，但是当前你 go ck 驱动只支持 Decimal64
			if M > 18 {
				ckType = "String"
			} else {
				var D int
				if len(p) == 2 {
					D, _ = strconv.Atoi(strings.Trim(p[1], " "))
				}
				ckType = fmt.Sprintf("Decimal(%d,%d)", M, D)
			}
		}
	case "real", "double":
		ckType = "Float64"
	case "float":
		ckType = "Float32"
	case "timestamp", "datetime":
		if dataTypeParam != "" {
			ckType = "DateTime64(" + dataTypeParam + ")"
		} else {
			ckType = "DateTime"
		}
	case "time":
		ckType = "String"
	case "date":
		ckType = "Date"
	default:
		ckType = "String"
	}
	return
}
