package src

import (
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
	Sql string
	c   *Conn
}

type AlterColumnInfo struct {
	isUnsigned bool
	AfterName string
	Default   *string
	Nullable  bool
	Comment   string
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
	str = strings.ReplaceAll(str,"#@%",",")
	return str
}

func NewAlterSQL(DefaultSchemaName,sql string,c *Conn) *AlterSQL {
	return &AlterSQL{
		DefaultSchemaName:DefaultSchemaName,
		Sql:sql,
		c : c,
		}
}

func (This *AlterSQL) Transfer2CkSQL() (SchemaName,TableName,destAlterSql string) {
	sql0 := TransferComma2Other(This.Sql)
	sql0Arr := strings.Split(sql0, ",")
	alterParamArr := make([]string,0)
	for i,v := range sql0Arr {
		v = ReplaceBr(v)
		v = strings.Trim(v," ")
		v = TransferOther2Comma(v)
		UpperV := strings.ToUpper(v)
		// 假如是第一个，则要去除  ALTER TABLE tableName
		if i == 0 && strings.Index(UpperV,"ALTER TABLE") == 0 {
			tmpArr := strings.Split(v," ")
			SchemaName,TableName = This.c.getAutoTableSqlSchemaAndTable(tmpArr[2],This.DefaultSchemaName)
			SchemaName = This.c.GetFieldName(SchemaName)
			TableName = This.c.GetFieldName(TableName)
			v = strings.Join(tmpArr[3:]," ")
			v = strings.Trim(v," ")
			UpperV = strings.ToUpper(v)
		}
		if strings.Index(UpperV,"CHANGE") == 0 {
			alterParamArr = append(alterParamArr,This.ChangeColumn(v))
			continue
		}
		if strings.Index(UpperV,"ADD") == 0 {
			alterParamArr = append(alterParamArr,This.AddColumn(v))
			continue
		}
		if strings.Index(UpperV,"MODIFY") == 0 {
			alterParamArr = append(alterParamArr,This.ModifyColumn(v))
			continue
		}
		/*
		if strings.Index(UpperV,"DROP COLUMN") == 0 {
			continue
			//alterParamArr = append(alterParamArr,This.DropColumn(v))
		}
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
	destAlterSql = "alter table `"+ SchemaName +"`.`"+ TableName +"` " + strings.Join(alterParamArr,",")
	return
}

func (This *AlterSQL) DropColumn(sql string) (destAlterSql string) {
	pArr := strings.Split(sql," ")
	return pArr[3]
}

/*
mysql : CHANGE `number` `number` BIGINT(20) NOT NULL  COMMENT '馆藏数量',
ck : modify column column_name [type] [default_expr]
*/
func (This *AlterSQL) ChangeColumn(sql string) (destAlterSql string) {
	var columnName, ckType string
	pArr := strings.Split(sql, " ")
	// 不支持修改字段名
	if pArr[1] != pArr[2] {
		return
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
		case "Float32","Float64","String":
			break
		default:
			ckType = "U"+ckType
		}
	}
	if AlterColumn.Nullable == true {
		ckType = " Nullable("+ckType+")"
	}
	destAlterSql = "modify column " + columnName + " " + ckType
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
	columnName = pArr[2]
	ckType = This.GetTransferCkType(pArr[3])
	var AlterColumn = &AlterColumnInfo{}
	if len(pArr) > 4 {
		AlterColumn = This.GetColumnInfo(pArr[4:])
	}

	if AlterColumn.isUnsigned {
		// mysql 里，float double ,decimal 是可以设置 unsigned
		switch ckType {
		case "Float32","Float64","String":
			break
		default:
			ckType = "U"+ckType
		}
	}
	if AlterColumn.Nullable == true {
		ckType = " Nullable("+ckType+")"
	}
	destAlterSql = "modify column " + columnName + " " + ckType
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
	if strings.Index(strings.ToUpper(sql),"ADD COLUMN") == 0 {
		columnNameIndex = 2
	}
	var columnName,ckType string
	pArr := strings.Split(sql," ")
	columnName = pArr[columnNameIndex]
	ckType = This.GetTransferCkType(pArr[columnNameIndex + 1])
	var AlterColumn = &AlterColumnInfo{}
	var columnOtherInfoIndex = columnNameIndex + 2
	if len(pArr) > columnOtherInfoIndex {
		AlterColumn = This.GetColumnInfo(pArr[columnOtherInfoIndex:])
	}

	if AlterColumn.isUnsigned {
		// mysql 里，float double ,decimal 是可以设置 unsigned
		switch ckType {
		case "Float32","Float64","String":
			break
		default:
			ckType = "U"+ckType
		}
	}
	if AlterColumn.Nullable == true {
		ckType = " Nullable("+ckType+")"
	}
	destAlterSql = "add column " + columnName + " " + ckType
	if AlterColumn.Comment != "" {
		destAlterSql += " COMMENT " + AlterColumn.Comment + ""
	}
	return
}

func (This *AlterSQL) GetColumnInfo(pArr []string) *AlterColumnInfo {
	AlterColumn := &AlterColumnInfo{ Nullable:false }
	var key string
	var val string
	var valFirst string
	for _, v := range pArr {
		UpperV := strings.ToUpper(v)
		if UpperV == "UNSIGNED" {
			AlterColumn.isUnsigned = true
			key,val,valFirst = "","",""
			continue
		}
		if key == "" {
			if UpperV == "NULL" {
				AlterColumn.Nullable = true
				key,val,valFirst = "","",""
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
				last = v[n-1:n]
			}
		}
		switch valFirst {
		case "'","\"":
			val += " "+v
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
				key,val,valFirst = "","",""
				break
			}
			val0 := val
			AlterColumn.Default = &val0
		case "COMMENT":
			AlterColumn.Comment = val
		case "AFTER":
			AlterColumn.AfterName = val
			key,val = "",""
		case "NOT":
			if UpperV == "NULL" {
				AlterColumn.Nullable = false
			}
			break
		default:
			break
		}
		key,val,valFirst = "","",""
	}
	return AlterColumn
}

func (This *AlterSQL) GetTransferCkType(mysqlColumnType string) (ckType string) {
	var mysqlDataType string
	var dataTypeParam string
	n := strings.Index(mysqlColumnType,"(")
	if n > 0 {
		mysqlDataType = strings.ToLower(mysqlColumnType[0:n])
		dataTypeParam = mysqlColumnType[n+1:len(mysqlColumnType)-1]
		dataTypeParam = strings.Trim(dataTypeParam," ")
	}else{
		mysqlDataType = strings.ToLower(mysqlColumnType)
	}
	switch mysqlDataType {
	case "tinyint":
		ckType = "Int8"
	case "smallint","year":
		ckType = "Int16"
	case "mediumint","int":
		ckType = "Int32"
	case "bigint":
		ckType = "Int64"
	case "numeric","decimal":
		if dataTypeParam == "" {
			ckType = "Decimal(18,2)"
		}else{
			p := strings.Split(dataTypeParam,",")
			M, _ := strconv.Atoi(strings.Trim(p[0],""))
			// M,D.   M > 18 就属于 Decimal128 , M > 39 就属于 Decimal256  ，但是当前你 go ck 驱动只支持 Decimal64
			if M > 18 {
				ckType = "String"
			}else{
				ckType = "Decimal("+dataTypeParam+")"
			}
		}
	case "real","double":
		ckType = "Float64"
	case "float":
		ckType = "Float32"
	case "timestamp","datetime":
		if dataTypeParam != "" {
			ckType = "DateTime64("+dataTypeParam+")"
		}else{
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