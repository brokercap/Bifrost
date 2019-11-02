package src

import (
	"github.com/brokercap/Bifrost/manager/xgo"
	"net/http"
	"encoding/json"
	"os/exec"
	"os"
	"path/filepath"
	"html/template"
	"strings"
)


var execDir string
func init()  {
	xgo.AddRoute("/bifrost/TableCount/index",TableCountIndex_Controller)
	xgo.AddRoute("/bifrost/TableCount/flow/get",TableCountFlow_Controller)
	xgo.AddRoute("/bifrost/TableCount/flow/schema/list",TableCountSchameList_Controller)
	xgo.AddRoute("/bifrost/TableCount/flow/table/list",TableCountSchameTableList_Controller)

	execPath, _ := exec.LookPath(os.Args[0])
	execDir = filepath.Dir(execPath)+"/"
}

type TemplateHeader struct {
	Title string
}


type resultDataStruct struct {
	Status bool `json:"status"`
	Msg string `json:"msg"`
	Data interface{} `json:"data"`
}

func TemplatePath(fileName string) string{
	return execDir+fileName
}

func TableCountIndex_Controller(w http.ResponseWriter,req *http.Request)  {
	req.ParseForm()
	type flowIndex struct{
		TemplateHeader
		DbList []string
	}

	FlowIndex := flowIndex{
		DbList:GetDbList(),
	}
	FlowIndex.Title = "FlowCount-Plugin-TableCount-Bifrost"
	t, _ := template.ParseFiles(TemplatePath("plugin/TableCount/www/flow.html"),TemplatePath("manager/template/header.html"),TemplatePath("manager/template/footer.html"))
	t.Execute(w, FlowIndex)
	return
}


func TableCountFlow_Controller(w http.ResponseWriter,req *http.Request)  {
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	schema := req.Form.Get("schema")
	tablename := req.Form.Get("table_name")
	FlowType := req.Form.Get("type")

	var Type  string
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
	if tablename != ""{
		data , err = GetFlow(Type,dbname,schema,tablename)
	}else{
		if schema != ""{
			data , err = GetFlowBySchema(Type,dbname,schema)
		}else{
			data , err = GetFlowByDbName(Type,dbname)
		}
	}

	result := &resultDataStruct{}

	if err != nil{
		result.Msg = err.Error()
		result.Status = false
	}else{
		result.Status = true
		result.Data = data
		result.Msg = "success"
	}
	b,_:= json.Marshal(result)
	w.Write(b)
}

func TableCountSchameList_Controller(w http.ResponseWriter,req *http.Request) {
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	data := GetSchameList(dbname)
	b,_:= json.Marshal(data)
	w.Write(b)
}

func TableCountSchameTableList_Controller(w http.ResponseWriter,req *http.Request) {
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	schema := req.Form.Get("schema")
	data := GetSchameTableList(dbname,schema)
	b,_:= json.Marshal(data)
	w.Write(b)
}