/*
Copyright [2018] [jc3wish]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package controller

import (
	"fmt"
	"github.com/brokercap/Bifrost/server"
	"github.com/brokercap/Bifrost/server/count"
)

type FlowController struct {
	CommonController
}

func (c *FlowController) Index() {
	DbName := c.Ctx.Request.Form.Get("DbName")
	SchemaName := c.Ctx.Request.Form.Get("SchemaName")
	TableName := c.Ctx.Request.Form.Get("TableName")
	ChannelId := c.Ctx.Request.Form.Get("ChannelId")

	c.SetTitle("Flow")
	c.SetData("DbName", DbName)
	c.SetData("SchemaName", SchemaName)
	c.SetData("TableName", TableName)
	c.SetData("ChannelId", ChannelId)
	c.AddAdminTemplate("flow.html", "header.html", "footer.html")
}

func (c *FlowController) GetFlow() {
	DbName := c.Ctx.Request.Form.Get("DbName")
	SchemaName := c.Ctx.Request.Form.Get("SchemaName")
	TableName := c.Ctx.Request.Form.Get("TableName")
	ChannelId := c.Ctx.Request.Form.Get("ChannelId")
	FlowType := c.Ctx.Request.Form.Get("Type")
	if FlowType == "" {
		FlowType = "minute"
	}
	SchemaName0 := tansferSchemaName(SchemaName)
	TableName0 := tansferTableName(TableName)
	dbANdTableName := server.GetSchemaAndTableJoin(SchemaName0, TableName0)
	var data []count.CountContent
	switch FlowType {
	case "minute":
		data, _ = c.getFlowCount(&DbName, &dbANdTableName, &ChannelId, "Minute")
		break
	case "tenminute":
		data, _ = c.getFlowCount(&DbName, &dbANdTableName, &ChannelId, "TenMinute")
		break
	case "hour":
		data, _ = c.getFlowCount(&DbName, &dbANdTableName, &ChannelId, "Hour")
		break
	case "eighthour":
		data, _ = c.getFlowCount(&DbName, &dbANdTableName, &ChannelId, "EightHour")
		break
	case "day":
		data, _ = c.getFlowCount(&DbName, &dbANdTableName, &ChannelId, "Day")
		break
	default:
		data = make([]count.CountContent, 0)
		break
	}
	c.SetJsonData(data)
	c.StopServeJSON()
}

func (c *FlowController) getFlowCount(dbname *string, dbANdTableName *string, channelId *string, FlowType string) ([]count.CountContent, error) {
	if *dbname == "" {
		return count.GetFlowAll(FlowType), nil
	}
	if *dbANdTableName != server.GetSchemaAndTableJoin("", "") {
		if *dbname == "" {
			return make([]count.CountContent, 0), fmt.Errorf("param error")
		}
		return count.GetFlowByTable(*dbname, *dbANdTableName, FlowType), nil
	}

	if *channelId != "" {
		return count.GetFlowByChannel(*dbname, *channelId, FlowType), nil
	}
	return count.GetFlowByDb(*dbname, FlowType), nil
}
