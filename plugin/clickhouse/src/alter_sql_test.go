package src

import (
	"strings"
	"testing"
)

func TestAlterSQL_ChangeColumn(t *testing.T) {
	sql := "CHANGE `number` `number` BIGINT(20) unsigned NULL COMMENT '馆藏数量'"
	c := NewAlterSQL("bifrost_test", "table_test", nil)

	var destAlterSql string
	destAlterSql = c.ChangeColumn(sql)
	if destAlterSql != "MODIFY COLUMN IF EXISTS `number`  Nullable(UInt64) COMMENT  '馆藏数量'" {
		t.Fatal("err destAlterSql:", destAlterSql)
	}
	t.Log("test success!")
}

func TestAlterSQL_AddColumn(t *testing.T) {
	sql := "ADD COLUMN `f1` VARCHAR(200) NULL AFTER `number`,"
	c := NewAlterSQL("bifrost_test", "table_test", nil)
	var destAlterSql string
	destAlterSql = c.AddColumn(sql)
	if destAlterSql != "add column IF NOT EXISTS `f1`  Nullable(String)" {
		t.Fatal("err destAlterSql:", destAlterSql)
	}
	t.Log("test success!")
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
  ADD INDEX sdfsdfsdf (id),
  ADD PRIMARY key(id),
 COMMENT='mytest\'s test,';
`

	ckObj := &Conn{
		p: &PluginParam{
			CkSchema:     "",
			ModifDDLType: &DDLSupportType{},
		},
	}
	ckObj.p.ModifDDLType.ColumnAdd = true
	ckObj.p.ModifDDLType.ColumnModify = true
	ckObj.p.ModifDDLType.TableRename = true

	ckObj.p.ModifDDLType.ColumnDrop = false
	ckObj.p.ModifDDLType.DropDbAndTable = false
	ckObj.p.ModifDDLType.Rruncate = false
	ckObj.p.CkEngine = 2
	ckObj.p.CkClusterName = "ck_cluster"

	Query := ReplaceBr(sql)
	Query = ReplaceTwoReplace(Query)
	Query = TransferNotes2Space(Query)
	Query = strings.Trim(strings.Trim(strings.Trim(Query, " "), ";"), " ")
	var destAlterSql, destLocalAlterSql, destDisAlterSql, destViewAlterSql string
	c := NewAlterSQL("test", sql, ckObj)
	_, _, destAlterSql, destLocalAlterSql, destDisAlterSql, destViewAlterSql = c.Transfer2CkSQL(ckObj)
	if destDisAlterSql == "" {
		t.Fatal("sql:", sql, " destAlterSql is empty!")
	}

	var mustBeDestLocalAlterSql = "alter table `bifrost_test_ck`.`mytest_local`  on cluster ck_cluster add column IF NOT EXISTS decimal_test Decimal(18,2),add column IF NOT EXISTS float_test Float32,add column IF NOT EXISTS double_test  Nullable(Float64) COMMENT  \"ffs,ssf\",add column IF NOT EXISTS f1  Nullable(String),add column IF NOT EXISTS decimal_test_1  Nullable(Decimal(18,2)),add column IF NOT EXISTS decimal_test_2  Nullable(Decimal(18,2)),add column IF NOT EXISTS decimal_test_3  Nullable(String)"
	var mustBeDestDisAlterSql = "alter table `bifrost_test_ck`.`mytest_all`  on cluster ck_cluster add column IF NOT EXISTS decimal_test Decimal(18,2),add column IF NOT EXISTS float_test Float32,add column IF NOT EXISTS double_test  Nullable(Float64) COMMENT  \"ffs,ssf\",add column IF NOT EXISTS f1  Nullable(String),add column IF NOT EXISTS decimal_test_1  Nullable(Decimal(18,2)),add column IF NOT EXISTS decimal_test_2  Nullable(Decimal(18,2)),add column IF NOT EXISTS decimal_test_3  Nullable(String)"
	var mustBeDestViewAlterSql = "Drop TABLE IF EXISTS bifrost_test_ck.mytest_all_pview on cluster ck_cluster;create view IF NOT EXISTS bifrost_test_ck.mytest_all_pview on cluster ck_cluster as select * from bifrost_test_ck.mytest_all final"

	if destAlterSql != "" {
		t.Errorf("destAlterSql is not empty!")
	}

	if destLocalAlterSql != mustBeDestLocalAlterSql {
		t.Errorf("err destLocalAlterSql: %s", destLocalAlterSql)
	}

	if destDisAlterSql != mustBeDestDisAlterSql {
		t.Errorf("err destLocalAlterSql: %s", destDisAlterSql)
	}

	if destViewAlterSql != mustBeDestViewAlterSql {
		t.Errorf("err destLocalAlterSql: %s", destViewAlterSql)
	}

	sql = `ALTER TABLE mytest
	ADD COLUMN f1 CHAR(10) DEFAULT ''  NOT NULL AFTER varchartest"`
	Query = ReplaceBr(sql)
	Query = ReplaceTwoReplace(Query)
	Query = TransferNotes2Space(Query)
	Query = strings.Trim(strings.Trim(strings.Trim(Query, " "), ";"), " ")
	c = NewAlterSQL("", Query, ckObj)
	_, _, destAlterSql, destLocalAlterSql, destDisAlterSql, destViewAlterSql = c.Transfer2CkSQL(ckObj)

	mustBeDestLocalAlterSql = "alter table `_ck`.`mytest_local`  on cluster ck_cluster add column IF NOT EXISTS f1 String"
	mustBeDestDisAlterSql = "alter table `_ck`.`mytest_all`  on cluster ck_cluster add column IF NOT EXISTS f1 String"
	mustBeDestViewAlterSql = "Drop TABLE IF EXISTS _ck.mytest_all_pview on cluster ck_cluster;create view IF NOT EXISTS _ck.mytest_all_pview on cluster ck_cluster as select * from _ck.mytest_all final"

	if destAlterSql != "" {
		t.Errorf("destAlterSql is not empty!")
	}

	if destLocalAlterSql != mustBeDestLocalAlterSql {
		t.Errorf("err destLocalAlterSql: %s", destLocalAlterSql)
	}

	if destDisAlterSql != mustBeDestDisAlterSql {
		t.Errorf("err destLocalAlterSql: %s", destDisAlterSql)
	}

	if destViewAlterSql != mustBeDestViewAlterSql {
		t.Errorf("err destLocalAlterSql: %s", destViewAlterSql)
	}

	sql = `ALTER TABLE bifrost_test.table_nodata   
  ADD COLUMN t1 TIMESTAMP DEFAULT '2020-01-12 21:00:00'		  NULL		COMMENT "it is test" AFTER f1;`
	Query = ReplaceBr(sql)
	Query = ReplaceTwoReplace(Query)
	Query = TransferNotes2Space(Query)
	Query = strings.Trim(strings.Trim(strings.Trim(Query, " "), ";"), " ")
	c = NewAlterSQL("test", Query, ckObj)
	_, _, destAlterSql, destLocalAlterSql, destDisAlterSql, destViewAlterSql = c.Transfer2CkSQL(ckObj)

	mustBeDestLocalAlterSql = "alter table `bifrost_test_ck`.`table_nodata_local`  on cluster ck_cluster add column IF NOT EXISTS t1  Nullable(DateTime) COMMENT  \"it is test\""
	mustBeDestDisAlterSql = "alter table `bifrost_test_ck`.`table_nodata_all`  on cluster ck_cluster add column IF NOT EXISTS t1  Nullable(DateTime) COMMENT  \"it is test\""
	mustBeDestViewAlterSql = "Drop TABLE IF EXISTS bifrost_test_ck.table_nodata_all_pview on cluster ck_cluster;create view IF NOT EXISTS bifrost_test_ck.table_nodata_all_pview on cluster ck_cluster as select * from bifrost_test_ck.table_nodata_all final"

	if destAlterSql != "" {
		t.Errorf("destAlterSql is not empty!")
	}

	if destLocalAlterSql != mustBeDestLocalAlterSql {
		t.Errorf("err destLocalAlterSql: %s", destLocalAlterSql)
	}

	if destDisAlterSql != mustBeDestDisAlterSql {
		t.Errorf("err destLocalAlterSql: %s", destDisAlterSql)
	}

	if destViewAlterSql != mustBeDestViewAlterSql {
		t.Errorf("err destLocalAlterSql: %s", destViewAlterSql)
	}

	sql = `ALTER TABLE /* it is notes */ binlog_field_test 
  CHANGE testtinyint testtinyint INT UNSIGNED DEFAULT -1  NOT NULL,
  CHANGE testvarchar testvarchar VARCHAR(60) CHARSET utf8 COLLATE utf8_general_ci NOT NULL,
  ADD COLUMN testint2 INT(11) DEFAULT 0  NOT NULL   COMMENT 'test ok' AFTER test_json,
  MODIFY COLUMN testint3 int DEFAULT 1 NULL comment 'sdfsdf sdf',
`
	Query = TransferNotes2Space(sql)
	Query = ReplaceBr(Query)
	Query = ReplaceTwoReplace(Query)

	t.Log("Query:", Query)
	Query = strings.Trim(strings.Trim(strings.Trim(Query, " "), ";"), " ")
	destAlterSql = ""
	c = NewAlterSQL("test", Query, ckObj)
	_, _, destAlterSql, destLocalAlterSql, destDisAlterSql, destViewAlterSql = c.Transfer2CkSQL(ckObj)

	mustBeDestLocalAlterSql = "alter table `test_ck`.`binlog_field_test_local`  on cluster ck_cluster MODIFY COLUMN IF EXISTS testtinyint UInt32,MODIFY COLUMN IF EXISTS testvarchar String,add column IF NOT EXISTS testint2  Nullable(Int32) COMMENT  'sdfsdf sdf'"
	mustBeDestDisAlterSql = "alter table `test_ck`.`binlog_field_test_all`  on cluster ck_cluster MODIFY COLUMN IF EXISTS testtinyint UInt32,MODIFY COLUMN IF EXISTS testvarchar String,add column IF NOT EXISTS testint2  Nullable(Int32) COMMENT  'sdfsdf sdf'"
	mustBeDestViewAlterSql = "Drop TABLE IF EXISTS test_ck.binlog_field_test_all_pview on cluster ck_cluster;create view IF NOT EXISTS test_ck.binlog_field_test_all_pview on cluster ck_cluster as select * from test_ck.binlog_field_test_all final"

	if destAlterSql != "" {
		t.Errorf("destAlterSql is not empty!")
	}

	if destLocalAlterSql != mustBeDestLocalAlterSql {
		t.Errorf("err destLocalAlterSql: %s", destLocalAlterSql)
	}

	if destDisAlterSql != mustBeDestDisAlterSql {
		t.Errorf("err destLocalAlterSql: %s", destDisAlterSql)
	}

	if destViewAlterSql != mustBeDestViewAlterSql {
		t.Errorf("err destLocalAlterSql: %s", destViewAlterSql)
	}

	t.Log("test over!")
}

func TestAlterSQL_Transfer2CkSQL_Unsupport(t *testing.T) {

	ckObj := &Conn{
		p: &PluginParam{
			CkSchema:     "",
			ModifDDLType: &DDLSupportType{},
		},
	}

	var f = func(Query string) {
		var newSql string
		Query = TransferNotes2Space(Query)
		Query = ReplaceBr(Query)
		Query = ReplaceTwoReplace(Query)
		c := NewAlterSQL("", Query, ckObj)
		_, _, newSql, _, _, _ = c.Transfer2CkSQL(ckObj)
		if newSql != "" {
			t.Fatal("Query:", Query, " newSql is not emtpy:", newSql)
		}
	}

	var sql string
	sql = `ALTER TABLE mytest
	ADD PRIMARY KEY ( column )`

	f(sql)

	sql = `ALTER TABLE mytest
	ADD UNIQUE KEY ( column )`

	f(sql)

	sql = `ALTER TABLE mytest
	ADD INdex index_name ( column )`

	f(sql)

	sql = `ALTER TABLE mytest
	ADD FOREIGN key ( column )`

	f(sql)

	sql = `ALTER TABLE mytest
	ADD PARTITION key ( column )`
	f(sql)

	sql = `ALTER TABLE mytest
	DROP PRIMARY KEY ( column )`

	f(sql)

	sql = `ALTER TABLE mytest
	DROP UNIQUE KEY ( column )`

	f(sql)

	sql = `ALTER TABLE mytest
	DROP INdex index_name ( column )`

	f(sql)

	sql = `ALTER TABLE mytest
	DROP FOREIGN key ( column )`

	f(sql)

	sql = `ALTER TABLE mytest
	DROP PARTITION key ( column )`
	f(sql)

}

func TestAlterSQL_GetColumnInfo(t *testing.T) {
	//sql := `ALTER TABLE bifrost_test.table_nodata
	//ADD COLUMN t1 TIMESTAMP DEFAULT '2020-01-12 121:00:00'  NULL  COMMENT "it is test" AFTER f1;`
	ckObj := &Conn{
		p: &PluginParam{
			CkSchema: "",
		},
	}
	sql0 := `ADD COLUMN t1 TIMESTAMP DEFAULT '2020-01-12 121:00:00'  NULL  COMMENT "it is test" AFTER f1;`
	Query := ReplaceTwoReplace(sql0)
	Query = strings.Trim(strings.Trim(strings.Trim(Query, " "), ";"), " ")

	pArr := strings.Split(Query, " ")
	c := NewAlterSQL("test", Query, ckObj)
	AlterColumnInfo := c.GetColumnInfo(pArr[4:])

	if AlterColumnInfo.isUnsigned != false {
		t.Errorf("isUnsigned must == false")
	}
	if AlterColumnInfo.AfterName != "f1" {
		t.Errorf("AfterName(%s) must == f1", AlterColumnInfo.AfterName)
	}
	if AlterColumnInfo.Nullable != true {
		t.Errorf("Nullable must == true")
	}
	if strings.Trim(AlterColumnInfo.Comment, " ") != "\"it is test\"" {
		t.Errorf("Comment(%s) must == 'it is test'", AlterColumnInfo.Comment)
	}
	if strings.Trim(*AlterColumnInfo.Default, " ") != "'2020-01-12 121:00:00'" {
		t.Errorf("Default(%s) must == '2020-01-12 121:00:00'", *AlterColumnInfo.Default)
	}

	t.Log("test over!")
}

func TestTransferComma2Other(t *testing.T) {
	sourceSql := `ALTER TABLE bifrost_test.mytest   
  ADD COLUMN decimal_test DECIMAL(18,2) DEFAULT 0.00  NOT NULL AFTER varchartest,
  ADD COLUMN float_test FLOAT(7,2) DEFAULT 0.00  NOT NULL AFTER decimal_test,
  ADD COLUMN double_test DOUBLE(9,2) DEFAULT 0.00  NULL AFTER float_test COMMENT "ffs,ssf",
COMMENT='mytest\'s test,';
`
	transferSQL := TransferComma2Other(sourceSql)
	newSQL := TransferOther2Comma(transferSQL)
	if sourceSql != newSQL {
		t.Fatalf("err newSQL: %s ", newSQL)
	}
	t.Log("test over!")

	sourceSql = "ADD COLUMN decimal_test DECIMAL(18,2) DEFAULT 0.00  NOT NULL AFTER varchartest,"
	transferSQL = TransferComma2Other(sourceSql)
	if transferSQL != "ADD COLUMN decimal_test DECIMAL(18#@%2) DEFAULT 0.00  NOT NULL AFTER varchartest," {
		t.Fatalf("err transferSQL: %s ", transferSQL)
	}

	sourceSql = `ADD COLUMN double_test DOUBLE(9#@%2) DEFAULT 0.00  NULL AFTER float_test COMMENT "ffs,ssf", COMMENT='mytest\'s test,';`
	transferSQL = TransferComma2Other(sourceSql)
	if transferSQL != `ADD COLUMN double_test DOUBLE(9#@%2) DEFAULT 0.00  NULL AFTER float_test COMMENT "ffs#@%ssf", COMMENT='mytest\'s test#@%';` {
		t.Fatalf("err transferSQL: %s ", transferSQL)
	}
}

func TestAlterSQL_GetTransferCkType(t *testing.T) {
	type result struct {
		Val   string
		Type  string
		IsErr bool
	}

	testArr := make([]result, 0)
	testArr = append(testArr, result{Val: "date", Type: "Date"})
	testArr = append(testArr, result{Val: "timestamp(5)", Type: "DateTime64(5)"})
	testArr = append(testArr, result{Val: "time(5)", Type: "String"})
	testArr = append(testArr, result{Val: "timestamp(5)", Type: "DateTime64(5)"})
	testArr = append(testArr, result{Val: "datetime(5)", Type: "DateTime64(5)"})
	testArr = append(testArr, result{Val: "datetime(6)", Type: "DateTime64(6)"})
	testArr = append(testArr, result{Val: "bigint", Type: "Int64"})
	testArr = append(testArr, result{Val: "decimal(3, 2)", Type: "Decimal(3,2)"})
	testArr = append(testArr, result{Val: "decimal( 18, 5)", Type: "Decimal(18,5)"})
	testArr = append(testArr, result{Val: "decimal( 38, 5)", Type: "String"})
	testArr = append(testArr, result{Val: "decimal( )", Type: "Decimal(18,2)"})
	testArr = append(testArr, result{Val: "decimal( 1 )", Type: "Decimal(1,0)"})

	ckObj := &Conn{
		p: &PluginParam{
			CkSchema: "",
		},
	}
	c := NewAlterSQL("test", "", ckObj)

	for _, v := range testArr {
		TypeName := c.GetTransferCkType(v.Val)
		if TypeName != v.Type {
			t.Error(v.Val, TypeName, "!=", v.Type, " ( need )")
			continue
		}
		t.Log(v.Val, v.Type, "success")
	}
}
