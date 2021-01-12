package src

import (
	"testing"
	"strings"
)

func TestAlterSQL_ChangeColumn(t *testing.T) {
	sql := "CHANGE `number` `number` BIGINT(20) unsigned NULL COMMENT '馆藏数量'"
	c := NewAlterSQL("bifrost_test","table_test",nil)

	var destAlterSql string
	destAlterSql  = c.ChangeColumn(sql)
	t.Log(destAlterSql)
}


func TestAlterSQL_AddColumn(t *testing.T) {
	sql := "ADD COLUMN `f1` VARCHAR(200) NULL AFTER `number`,"
	c := NewAlterSQL("bifrost_test","table_test",nil)
	var destAlterSql string
	destAlterSql  = c.AddColumn(sql)
	t.Log(destAlterSql)
}

func TestAlterSQL_Transfer2CkSQL(t *testing.T) {
	sql := "  ALTER TABLE `test`.`book` "
	sql += " DROP COLUMN `name`,"
	sql += " CHANGE `number` `number` BIGINT(20) NOT NULL  COMMENT '馆藏数量',"
	sql += " ADD COLUMN `f1` VARCHAR(200) NULL AFTER `number`,"
	sql += " ADD  INDEX `sdfsdfsdf` (`number`);"

	ckObj := &Conn{
		p:&PluginParam{
			CkSchema:"",
			},
		}
	Query := ReplaceBr(sql)
	Query = ReplaceTwoReplace(Query)
	Query = strings.Trim(strings.Trim(strings.Trim(Query," "),";")," ")
	var destAlterSql string
	c := NewAlterSQL("test",sql,ckObj)
	_,_,destAlterSql = c.Transfer2CkSQL()
	t.Log(destAlterSql)

	sql = `ALTER TABLE mytest
	ADD COLUMN f1 CHAR(10) DEFAULT ''  NOT NULL AFTER varchartest"`
	Query = ReplaceBr(sql)
	Query = ReplaceTwoReplace(Query)
	Query = strings.Trim(strings.Trim(strings.Trim(Query," "),";")," ")
	c = NewAlterSQL("",Query,ckObj)
	_,_,destAlterSql = c.Transfer2CkSQL()
	t.Log(destAlterSql)

	sql = `ALTER TABLE bifrost_test.table_nodata   
  ADD COLUMN t1 TIMESTAMP DEFAULT '2020-01-12 21:00:00'		  NULL		COMMENT "it is test" AFTER f1;`
	Query = ReplaceBr(sql)
	Query = ReplaceTwoReplace(Query)
	Query = strings.Trim(strings.Trim(strings.Trim(Query," "),";")," ")
	c = NewAlterSQL("test",sql,ckObj)
	_,_,destAlterSql = c.Transfer2CkSQL()
	t.Log("33:",destAlterSql)

}

func TestAlterSQL_GetColumnInfo(t *testing.T) {
	//sql := `ALTER TABLE bifrost_test.table_nodata
 //ADD COLUMN t1 TIMESTAMP DEFAULT '2020-01-12 121:00:00'  NULL  COMMENT "it is test" AFTER f1;`
	ckObj := &Conn{
		p:&PluginParam{
			CkSchema:"",
		},
	}
	sql0 := `ADD COLUMN t1 TIMESTAMP DEFAULT '2020-01-12 121:00:00'  NULL  COMMENT "it is test" AFTER f1;`
	Query := ReplaceTwoReplace(sql0)
	Query = strings.Trim(strings.Trim(strings.Trim(Query," "),";")," ")

	pArr := strings.Split(Query," ")
	c := NewAlterSQL("test",Query,ckObj)
	AlterColumnInfo := c.GetColumnInfo(pArr[4:])

	t.Log("AlterColumnInfo:",*AlterColumnInfo)
	if AlterColumnInfo.Default != nil {
		t.Log("default:",*AlterColumnInfo.Default)
	}
}