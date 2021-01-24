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
	sql := `ALTER TABLE bifrost_test.mytest   
  ADD COLUMN decimal_test DECIMAL(18,2) DEFAULT 0.00  NOT NULL AFTER varchartest,
  ADD COLUMN float_test FLOAT(7,2) DEFAULT 0.00  NOT NULL AFTER decimal_test,
  ADD COLUMN double_test DOUBLE(9,2) DEFAULT 0.00  NULL AFTER float_test COMMENT "ffs,ssf",
  ADD COLUMN f1 VARCHAR(200) NULL AFTER double_test,
  ADD COLUMN decimal_test_1 decimal(18,2) NULL AFTER f1,
  ADD COLUMN decimal_test_2 decimal NULL AFTER decimal_test_1,
  ADD COLUMN decimal_test_3 decimal(19,17) NULL AFTER decimal_test_2,
  ADD  INDEX sdfsdfsdf (id),
 COMMENT='mytest\'s test,';
`

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


	sql = `ALTER TABLE binlog_field_test 
  CHANGE testtinyint testtinyint INT UNSIGNED DEFAULT -1  NOT NULL,
  CHANGE testvarchar testvarchar VARCHAR(60) CHARSET utf8 COLLATE utf8_general_ci NOT NULL,
  ADD COLUMN testint2 INT(11) DEFAULT 0  NOT NULL   COMMENT 'test ok' AFTER test_json;
`
	Query = ReplaceBr(sql)
	Query = ReplaceTwoReplace(Query)
	Query = strings.Trim(strings.Trim(strings.Trim(Query," "),";")," ")
	destAlterSql = ""
	c = NewAlterSQL("test",sql,ckObj)
	_,_,destAlterSql = c.Transfer2CkSQL()
	t.Log(destAlterSql)
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

func TestTransferComma2Other(t *testing.T) {
	sql := `ALTER TABLE bifrost_test.mytest   
  ADD COLUMN decimal_test DECIMAL(18,2) DEFAULT 0.00  NOT NULL AFTER varchartest,
  ADD COLUMN float_test FLOAT(7,2) DEFAULT 0.00  NOT NULL AFTER decimal_test,
  ADD COLUMN double_test DOUBLE(9,2) DEFAULT 0.00  NULL AFTER float_test COMMENT "ffs,ssf",
COMMENT='mytest\'s test,';
`
	sql = TransferComma2Other(sql)
	t.Log(sql)
}