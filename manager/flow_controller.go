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
package manager

import (
	"net/http"
	"github.com/brokercap/Bifrost/server/count"
	"fmt"
	"encoding/json"
	"html/template"
	"github.com/brokercap/Bifrost/server"
)

func init(){
	addRoute("/flow/get",get_flow_controller)
	addRoute("/flow/index",index_flow_controller)
}

func index_flow_controller(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	type flowIndex struct{
		TemplateHeader
		DbName string
		Schema string
		TableName string
		ChannelId string
	}
	dbname := req.Form.Get("dbname")
	schema := req.Form.Get("schema")
	tablename := req.Form.Get("table_name")
	channelId := req.Form.Get("channelid")

	FlowIndex := flowIndex{
		DbName:dbname,
		Schema:schema,
		TableName:tablename,
		ChannelId:channelId,
		}
	FlowIndex.Title = "Flow-Bifrost"
	t, _ := template.ParseFiles(TemplatePath("manager/template/flow.html"),TemplatePath("manager/template/header.html"),TemplatePath("manager/template/footer.html"))
	t.Execute(w, FlowIndex)

}

func get_flow_controller(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	schema := req.Form.Get("schema")
	tablename := req.Form.Get("table_name")
	channelId := req.Form.Get("channelid")
	FlowType := req.Form.Get("type")
	if FlowType == ""{
		FlowType = "minute"
	}
	dbANdTableName := server.GetSchemaAndTableJoin(schema,tablename)
	var data []count.CountContent
	switch FlowType {
	case "minute":
		data,_=getFlowCount(&dbname,&dbANdTableName,&channelId,"Minute")
		break
	case "tenminute":
		data,_=getFlowCount(&dbname,&dbANdTableName,&channelId,"TenMinute")
		break
	case "hour":
		data,_=getFlowCount(&dbname,&dbANdTableName,&channelId,"Hour")
		break
	case "eighthour":
		data,_=getFlowCount(&dbname,&dbANdTableName,&channelId,"EightHour")
		break
	case "day":
		data,_=getFlowCount(&dbname,&dbANdTableName,&channelId,"Day")
		break
	default:
		data = make([]count.CountContent,0)
		break
	}
	b,_:=json.Marshal(data)
	w.Write(b)
}

func getFlowCount(dbname *string,dbANdTableName *string,channelId *string,FlowType string) ([]count.CountContent,error){
	if *dbname == ""{
		return count.GetFlowAll(FlowType),nil
	}
	if *dbANdTableName != server.GetSchemaAndTableJoin("",""){
		if *dbname == ""{
			return make([]count.CountContent,0),fmt.Errorf("param error")
		}
		return count.GetFlowByTable(*dbname,*dbANdTableName,FlowType),nil
	}

	if *channelId != ""{
		return count.GetFlowByChannel(*dbname,*channelId,FlowType),nil
	}
	return count.GetFlowByDb(*dbname,FlowType),nil
}