package src_test

import "testing"

import (
	MyPlugin "github.com/brokercap/Bifrost/plugin/clickhouse/src"
	"github.com/brokercap/Bifrost/sdk/pluginTestData"
	"database/sql/driver"
	"time"
)


func TestTransferToCreateTableSql(t *testing.T) {
	data := pluginTestData.NewEvent().GetTestInsertData()
	sql,ckField := MyPlugin.TransferToCreateTableSql(data.SchemaName,data.TableName,data.Rows[0],data.Pri)
	t.Log(sql)
	t.Log(ckField)
	c := MyPlugin.NewClickHouseDBConn(url)
	err := c.Exec(sql,[]driver.Value{})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("success")
}

func TestTransferToCreateDatabaseSql(t *testing.T) {
	sql := MyPlugin.TransferToCreateDatabaseSql("mytest2")
	t.Log(sql)
	c := MyPlugin.NewClickHouseDBConn(url)
	err := c.Exec(sql,[]driver.Value{})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("success")
}

func TestDateStringTransfer(t *testing.T) {

	var f = func(str string) (string,error){
		var err error
		var time0 time.Time
		var Result string = "String"
		switch len(str) {
		case 19:
			time0, err = time.Parse("2006-01-02 15:04:05", str)
			if err != nil {
				return "String",err
			}
			Result = "DateTime"
			t.Log(time0.String())
			break
		case 10:
			time0, err = time.Parse("2006-01-02", str)
			if err != nil {
				return "String",err
			}
			Result = "Date"
			t.Log(time0.String())
			break
		default:
			break
		}
		return Result,err
	}

	type result struct{
		Val  string
		Type string
	}

	testArr := make([]result,0)
	testArr = append(testArr,result{Val:"2006-01-08",Type:"Date"})
	testArr = append(testArr,result{Val:"2006-01-08 00:05:20",Type:"DateTime"})
	testArr = append(testArr,result{Val:"00:05:20",Type:"String"})
	testArr = append(testArr,result{Val:"2006-01-32 00:05:20",Type:"String"})

	for _,v := range testArr {
		TypeName,err := f(v.Val)
		if err != nil {
			t.Error(v.Val,"err:",err)
			continue
		}
		if TypeName != v.Type {
			t.Error(v.Val,TypeName, "!=",v.Type," ( need )")
			continue
		}
		t.Log(v.Val,v.Type, "success")
	}
}