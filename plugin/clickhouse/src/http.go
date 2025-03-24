package src

import (
	"github.com/brokercap/Bifrost/admin/controller"
	"github.com/brokercap/Bifrost/admin/xgo"
	pluginStorage "github.com/brokercap/Bifrost/plugin/storage"
)

func init() {
	xgo.Router("/bifrost/plugin/clickhouse/schemalist", &PluginClickHouseController{}, "*:GetClickHouseSchemaList")
	xgo.Router("/bifrost/plugin/clickhouse/tablelist", &PluginClickHouseController{}, "*:GetClickHouseSchemaTableList")
	xgo.Router("/bifrost/plugin/clickhouse/tableinfo", &PluginClickHouseController{}, "*:GetClickHouseTableFields")
}

type PluginClickHouseController struct {
	controller.CommonController
}

func (c *PluginClickHouseController) getToServerInfo() *pluginStorage.ToServer {
	ToServerKey := c.Ctx.Get("ToServerKey")
	toServerInfo := pluginStorage.GetToServerInfo(ToServerKey)
	if toServerInfo == nil {
		c.SetJsonData(ToServerKey + " no found")
		c.StopServeJSON()
		return nil
	}
	return toServerInfo
}

func (c *PluginClickHouseController) GetClickHouseSchemaList() {
	toServerInfo := c.getToServerInfo()
	conn := NewClickHouseDBConn(toServerInfo.ConnUri)
	defer conn.Close()
	SchemaList := conn.GetSchemaList()
	c.SetJsonData(SchemaList)
	c.StopServeJSON()
	return
}

func (c *PluginClickHouseController) GetClickHouseSchemaTableList() {
	toServerInfo := c.getToServerInfo()
	SchemaName := c.Ctx.Get("SchemaName")
	conn := NewClickHouseDBConn(toServerInfo.ConnUri)
	defer conn.Close()
	TableList := conn.GetSchemaTableList(SchemaName)
	c.SetJsonData(TableList)
	c.StopServeJSON()
	return
}

func (c *PluginClickHouseController) GetClickHouseTableFields() {
	toServerInfo := c.getToServerInfo()
	SchemaName := c.Ctx.Get("SchemaName")
	TableName := c.Ctx.Get("TableName")
	conn := NewClickHouseDBConn(toServerInfo.ConnUri)
	defer conn.Close()
	TableFieldMap := conn.GetTableFields(SchemaName, TableName)
	c.SetJsonData(TableFieldMap)
	c.StopServeJSON()
	return
}
