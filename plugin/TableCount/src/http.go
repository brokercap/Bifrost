package src

import (
	"github.com/brokercap/Bifrost/admin/controller"
	"github.com/brokercap/Bifrost/admin/xgo"
	"strings"
)

func init() {
	xgo.Router("/bifrost/plugin/TableCount/index", &PluginTableCountController{}, "*:Index")
	xgo.Router("/bifrost/plugin/TableCount/flow/get", &PluginTableCountController{}, "*:GetFlow")
	xgo.Router("/bifrost/plugin/TableCount/flow/schema/list", &PluginTableCountController{}, "*:GetSchemaList")
	xgo.Router("/bifrost/plugin/TableCount/flow/table/list", &PluginTableCountController{}, "*:GetSchemaTableList")
}

type PluginTableCountController struct {
	controller.CommonController
}

func (c *PluginTableCountController) Index() {
	c.SetTitle("FlowCount-Plugin-TableCount")
	c.SetData("DbList", GetDbList())
	c.AddPluginTemplate("TableCount/www/flow.html")
	c.AddAdminTemplate("header.html", "footer.html")
	return
}

func (c *PluginTableCountController) GetFlow() {
	DbName := c.Ctx.Get("DbName", "")
	SchemaName := c.Ctx.Get("SchemaName", "")
	TableName := c.Ctx.Get("TableName", "")
	FlowType := c.Ctx.Get("Type", "tenminute")
	var Type string
	switch strings.ToLower(FlowType) {
	case "tenminute":
		Type = "TenMinute"
		break
	case "hour":
		Type = "Hour"
		break
	case "eighthour":
		Type = "EightHour"
		break
	case "day":
		Type = "Day"
		break
	default:
		Type = "TenMinute"
		break
	}

	var data []CountContent
	var err error
	if TableName != "" {
		data, err = GetFlow(Type, DbName, SchemaName, TableName)
	} else {
		if SchemaName != "" {
			data, err = GetFlowBySchema(Type, DbName, SchemaName)
		} else {
			data, err = GetFlowByDbName(Type, DbName)
		}
	}

	result := &controller.ResultDataStruct{}

	if err != nil {
		result.Msg = err.Error()
		result.Status = 0
	} else {
		result.Status = 1
		result.Data = data
		result.Msg = "success"
	}
	c.SetJsonData(result)
	c.StopServeJSON()
}

func (c *PluginTableCountController) GetSchemaList() {
	DbName := c.Ctx.Get("DbName")
	data := GetSchameList(DbName)
	c.SetJsonData(data)
	c.StopServeJSON()
}

func (c *PluginTableCountController) GetSchemaTableList() {
	DbName := c.Ctx.Get("DbName")
	SchemaName := c.Ctx.Get("SchemaName")
	data := GetSchameTableList(DbName, SchemaName)
	c.SetJsonData(data)
	c.StopServeJSON()
}
