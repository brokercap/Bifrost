package kafka

import (
	"github.com/brokercap/Bifrost/Bristol/mysql"
	inputDriver "github.com/brokercap/Bifrost/input/driver"
)

func (c *Input) GetConn() mysql.MysqlConnection {
	db := mysql.NewConnect(c.inputInfo.ConnectUri)
	return db
}

func (c *Input) GetSchemaList() ([]string, error) {
	return make([]string,0),nil
}

func (c *Input) GetSchemaTableList(schema string) (tableList []inputDriver.TableList, err error) {
	return make([]inputDriver.TableList,0),nil
}

func (c *Input) GetSchemaTableFieldList(schema string, table string) (FieldList []inputDriver.TableFieldInfo, err error) {
	return make([]inputDriver.TableFieldInfo,0),nil
}

func (c *Input) CheckPrivilege() (err error) {
	return
}

func (c *Input) CheckUri(CheckPrivilege bool) (CheckUriResult inputDriver.CheckUriResult, err error) {
	result := inputDriver.CheckUriResult{
		BinlogFile:"bifrost.000001",
		BinlogPosition:0,
		Gtid:"",
		ServerId:1,
		BinlogFormat:"row",
		BinlogRowImage:"full",
	}
	return result,nil
}

func (c *Input) GetCurrentPosition() (p *inputDriver.PluginPosition, err error) {
	return
}

func (c *Input) GetVersion() (Version string, err error) {
	return
}
