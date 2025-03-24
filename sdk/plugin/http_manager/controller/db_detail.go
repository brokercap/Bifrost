package controller

import (
	"github.com/brokercap/Bifrost/admin/xgo"
	toserver "github.com/brokercap/Bifrost/plugin/storage"
	"html/template"
)

type DBController struct {
	xgo.Controller
}

func (c *DBController) Detail() {
	DbName := c.Ctx.Get("DbName")
	DataBaseList := []string{"bifrost_test"}

	c.SetData("DbName", DbName)
	c.SetData("DataBaseList", DataBaseList)
	c.SetData("ToServerList", toserver.GetToServerMap())
	c.SetData("ChannelList:", make(map[int]interface{}, 0))
	c.SetData("Title", DbName+" - Detail")

	var err error
	t := template.New("detail_html")
	t, err = t.Parse(IndexHtml)
	c.SetTemplate(t, err)
}

func (c *DBController) TableList() {
	type ResultType struct {
		TableName   string
		ChannelName string
		AddStatus   bool
		TableType   string
		IgnoreTable string
	}
	var data []ResultType
	data = make([]ResultType, 0)
	data = append(data, ResultType{TableName: "binlog_field_test", ChannelName: "default", AddStatus: true, TableType: ""})
	data = append(data, ResultType{TableName: "AllTables", ChannelName: "default", AddStatus: true, TableType: "LIKE"})
	c.SetJsonData(data)
	c.StopServeJSON()
}

func (c *DBController) GetTableFields() {
	b := `[{"COLUMN_NAME":"id","COLUMN_DEFAULT":"NULL","IS_NULLABLE":"NO","COLUMN_TYPE":"int(11) unsigned","COLUMN_KEY":"PRI","EXTRA":"auto_increment","COLUMN_COMMENT":"","DATA_TYPE":"int","NUMERIC_PRECISION":"10","NUMERIC_SCALE":"0"},{"COLUMN_NAME":"testtinyint","COLUMN_DEFAULT":"-1","IS_NULLABLE":"NO","COLUMN_TYPE":"tinyint(4)","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"tinyint","NUMERIC_PRECISION":"3","NUMERIC_SCALE":"0"},{"COLUMN_NAME":"testsmallint","COLUMN_DEFAULT":"-2","IS_NULLABLE":"NO","COLUMN_TYPE":"smallint(6)","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"smallint","NUMERIC_PRECISION":"5","NUMERIC_SCALE":"0"},{"COLUMN_NAME":"testmediumint","COLUMN_DEFAULT":"-3","IS_NULLABLE":"NO","COLUMN_TYPE":"mediumint(8)","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"mediumint","NUMERIC_PRECISION":"7","NUMERIC_SCALE":"0"},{"COLUMN_NAME":"testint","COLUMN_DEFAULT":"-4","IS_NULLABLE":"NO","COLUMN_TYPE":"int(11)","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"int","NUMERIC_PRECISION":"10","NUMERIC_SCALE":"0"},{"COLUMN_NAME":"testbigint","COLUMN_DEFAULT":"-5","IS_NULLABLE":"NO","COLUMN_TYPE":"bigint(20)","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"bigint","NUMERIC_PRECISION":"19","NUMERIC_SCALE":"0"},{"COLUMN_NAME":"testvarchar","COLUMN_DEFAULT":"NULL","IS_NULLABLE":"NO","COLUMN_TYPE":"varchar(10)","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"varchar","NUMERIC_PRECISION":"NULL","NUMERIC_SCALE":"NULL"},{"COLUMN_NAME":"testchar","COLUMN_DEFAULT":"NULL","IS_NULLABLE":"NO","COLUMN_TYPE":"char(2)","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"char","NUMERIC_PRECISION":"NULL","NUMERIC_SCALE":"NULL"},{"COLUMN_NAME":"testenum","COLUMN_DEFAULT":"en1","IS_NULLABLE":"NO","COLUMN_TYPE":"enum('en1','en2','en3')","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"enum","NUMERIC_PRECISION":"NULL","NUMERIC_SCALE":"NULL"},{"COLUMN_NAME":"testset","COLUMN_DEFAULT":"set1","IS_NULLABLE":"NO","COLUMN_TYPE":"set('set1','set2','set3')","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"set","NUMERIC_PRECISION":"NULL","NUMERIC_SCALE":"NULL"},{"COLUMN_NAME":"testtime","COLUMN_DEFAULT":"00:00:00","IS_NULLABLE":"NO","COLUMN_TYPE":"time","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"time","NUMERIC_PRECISION":"NULL","NUMERIC_SCALE":"NULL"},{"COLUMN_NAME":"testdate","COLUMN_DEFAULT":"0000-00-00","IS_NULLABLE":"NO","COLUMN_TYPE":"date","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"date","NUMERIC_PRECISION":"NULL","NUMERIC_SCALE":"NULL"},{"COLUMN_NAME":"testyear","COLUMN_DEFAULT":"1989","IS_NULLABLE":"NO","COLUMN_TYPE":"year(4)","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"year","NUMERIC_PRECISION":"NULL","NUMERIC_SCALE":"NULL"},{"COLUMN_NAME":"testtimestamp","COLUMN_DEFAULT":"CURRENT_TIMESTAMP","IS_NULLABLE":"NO","COLUMN_TYPE":"timestamp","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"timestamp","NUMERIC_PRECISION":"NULL","NUMERIC_SCALE":"NULL"},{"COLUMN_NAME":"testdatetime","COLUMN_DEFAULT":"0000-00-00 00:00:00","IS_NULLABLE":"NO","COLUMN_TYPE":"datetime","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"datetime","NUMERIC_PRECISION":"NULL","NUMERIC_SCALE":"NULL"},{"COLUMN_NAME":"testfloat","COLUMN_DEFAULT":"0.00","IS_NULLABLE":"NO","COLUMN_TYPE":"float(9,2)","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"float","NUMERIC_PRECISION":"9","NUMERIC_SCALE":"2"},{"COLUMN_NAME":"testdouble","COLUMN_DEFAULT":"0.00","IS_NULLABLE":"NO","COLUMN_TYPE":"double(9,2)","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"double","NUMERIC_PRECISION":"9","NUMERIC_SCALE":"2"},{"COLUMN_NAME":"testdecimal","COLUMN_DEFAULT":"0.00","IS_NULLABLE":"NO","COLUMN_TYPE":"decimal(9,2)","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"decimal","NUMERIC_PRECISION":"9","NUMERIC_SCALE":"2"},{"COLUMN_NAME":"testtext","COLUMN_DEFAULT":"NULL","IS_NULLABLE":"NO","COLUMN_TYPE":"text","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"text","NUMERIC_PRECISION":"NULL","NUMERIC_SCALE":"NULL"},{"COLUMN_NAME":"testblob","COLUMN_DEFAULT":"NULL","IS_NULLABLE":"NO","COLUMN_TYPE":"blob","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"blob","NUMERIC_PRECISION":"NULL","NUMERIC_SCALE":"NULL"},{"COLUMN_NAME":"testbit","COLUMN_DEFAULT":"b'0'","IS_NULLABLE":"NO","COLUMN_TYPE":"bit(8)","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"bit","NUMERIC_PRECISION":"8","NUMERIC_SCALE":"NULL"},{"COLUMN_NAME":"testbool","COLUMN_DEFAULT":"0","IS_NULLABLE":"NO","COLUMN_TYPE":"tinyint(1)","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"tinyint","NUMERIC_PRECISION":"3","NUMERIC_SCALE":"0"},{"COLUMN_NAME":"testmediumblob","COLUMN_DEFAULT":"NULL","IS_NULLABLE":"NO","COLUMN_TYPE":"mediumblob","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"mediumblob","NUMERIC_PRECISION":"NULL","NUMERIC_SCALE":"NULL"},{"COLUMN_NAME":"testlongblob","COLUMN_DEFAULT":"NULL","IS_NULLABLE":"NO","COLUMN_TYPE":"longblob","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"longblob","NUMERIC_PRECISION":"NULL","NUMERIC_SCALE":"NULL"},{"COLUMN_NAME":"testtinyblob","COLUMN_DEFAULT":"NULL","IS_NULLABLE":"NO","COLUMN_TYPE":"tinyblob","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"tinyblob","NUMERIC_PRECISION":"NULL","NUMERIC_SCALE":"NULL"},{"COLUMN_NAME":"test_unsinged_tinyint","COLUMN_DEFAULT":"1","IS_NULLABLE":"NO","COLUMN_TYPE":"tinyint(4) unsigned","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"tinyint","NUMERIC_PRECISION":"3","NUMERIC_SCALE":"0"},{"COLUMN_NAME":"test_unsinged_smallint","COLUMN_DEFAULT":"2","IS_NULLABLE":"NO","COLUMN_TYPE":"smallint(6) unsigned","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"smallint","NUMERIC_PRECISION":"5","NUMERIC_SCALE":"0"},{"COLUMN_NAME":"test_unsinged_mediumint","COLUMN_DEFAULT":"3","IS_NULLABLE":"NO","COLUMN_TYPE":"mediumint(8) unsigned","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"mediumint","NUMERIC_PRECISION":"7","NUMERIC_SCALE":"0"},{"COLUMN_NAME":"test_unsinged_int","COLUMN_DEFAULT":"4","IS_NULLABLE":"NO","COLUMN_TYPE":"int(11) unsigned","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"int","NUMERIC_PRECISION":"10","NUMERIC_SCALE":"0"},{"COLUMN_NAME":"test_unsinged_bigint","COLUMN_DEFAULT":"5","IS_NULLABLE":"NO","COLUMN_TYPE":"bigint(20) unsigned","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"bigint","NUMERIC_PRECISION":"20","NUMERIC_SCALE":"0"}]`
	c.SetOutputByUser()
	c.Ctx.ResponseWriter.Write([]byte(b))
}
