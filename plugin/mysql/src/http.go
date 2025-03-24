package src

import (
	"github.com/brokercap/Bifrost/admin/controller"
	"github.com/brokercap/Bifrost/admin/xgo"
	pluginStorage "github.com/brokercap/Bifrost/plugin/storage"
)

func init() {
	xgo.Router("/bifrost/plugin/mysql/schemalist", &PluginMySQLController{}, "*:GetMysqlSchemaList")
	xgo.Router("/bifrost/plugin/mysql/tablelist", &PluginMySQLController{}, "*:GetMysqlSchemaTableList")
	xgo.Router("/bifrost/plugin/mysql/tableinfo", &PluginMySQLController{}, "*:GetMysqlTableFields")
}

type PluginMySQLController struct {
	controller.CommonController
}

func (c *PluginMySQLController) getToServerInfo() *pluginStorage.ToServer {
	ToServerKey := c.Ctx.Get("ToServerKey")
	toServerInfo := pluginStorage.GetToServerInfo(ToServerKey)
	if toServerInfo == nil {
		c.SetJsonData(ToServerKey + " no found")
		c.StopServeJSON()
		return nil
	}
	return toServerInfo
}

func (c *PluginMySQLController) GetMysqlSchemaList() {
	toServerInfo := c.getToServerInfo()
	conn := NewMysqlDBConn(toServerInfo.ConnUri)
	defer conn.Close()
	SchemaList := conn.GetSchemaList()
	c.SetJsonData(SchemaList)
	c.StopServeJSON()
	return
}

func (c *PluginMySQLController) GetMysqlSchemaTableList() {
	toServerInfo := c.getToServerInfo()
	SchemaName := c.Ctx.Get("SchemaName")
	conn := NewMysqlDBConn(toServerInfo.ConnUri)
	defer conn.Close()
	TableList := conn.GetSchemaTableList(SchemaName)
	c.SetJsonData(TableList)
	c.StopServeJSON()
	return
}

func (c *PluginMySQLController) GetMysqlTableFields() {
	toServerInfo := c.getToServerInfo()
	SchemaName := c.Ctx.Get("SchemaName")
	TableName := c.Ctx.Get("TableName")
	conn := NewMysqlDBConn(toServerInfo.ConnUri)
	defer conn.Close()
	TableFieldMap := conn.GetTableFields(SchemaName, TableName)
	c.SetJsonData(TableFieldMap)
	c.StopServeJSON()
	return
}
