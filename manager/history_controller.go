package manager

import (
	"net/http"
	"html/template"
	"github.com/jc3wish/Bifrost/server/history"
	"encoding/json"
)
func init(){
	addRoute("/history/list",history_list_controller)
	addRoute("/history/add",history_add_controller)
	addRoute("/history/stop",history_kill_controller)
	addRoute("/history/del",history_del_controller)
	addRoute("/history/start",history_start_controller)
}

func history_list_controller(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	tablename := req.Form.Get("table_name")
	schema := req.Form.Get("schema_name")
	HistoryList := history.GetHistoryList(dbname,schema,tablename)

	if req.Form.Get("format") == "json"{
		b, _:= json.Marshal(HistoryList)
		w.Write(b)
		return
	}

	type HistoryListInfo struct {
		TemplateHeader
		DbName string
		SchemaName string
		TableName string
		HistoryList []history.History
	}
	var data HistoryListInfo
	data = HistoryListInfo{
		DbName:dbname,
		TableName:tablename,
		SchemaName:schema,
		HistoryList:HistoryList,
	}

	data.Title = "History List - Bifrost"
	t, _ := template.ParseFiles(TemplatePath("manager/template/history.list.html"),TemplatePath("manager/template/header.html"),TemplatePath("manager/template/footer.html"))
	t.Execute(w, data)
}


func history_add_controller(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	tablename := req.Form.Get("table_name")
	schema := req.Form.Get("schema_name")
	property := req.Form.Get("property")
	ToServerIDList := req.Form.Get("ToserverIds")
	var err error
	var Property history.HistoryProperty
	err = json.Unmarshal([]byte(property),&Property)
	if err != nil{
		w.Write(returnDataResult(false,err.Error(),0))
		return
	}
	var ToserverIds []int
	err = json.Unmarshal([]byte(ToServerIDList),&ToserverIds)
	if err != nil {
		w.Write(returnDataResult(false, err.Error(), 0))
		return
	}
	if len(ToserverIds) == 0 {
		w.Write(returnDataResult(false, "ToserverIds error", 0))
		return
	}

	var ID int
	ID,err = history.AddHistory(dbname,schema,tablename,Property,ToserverIds)
	if err != nil{
		w.Write(returnDataResult(false,err.Error(),0))
	}else{
		w.Write(returnDataResult(true,"success",ID))
	}
}

func history_del_controller(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	id := GetFormInt(req,"id")
	if id == 0{
		w.Write(returnResult(false,"id error not be int"))
		return
	}

	b := history.DelHistory(dbname,id)

	if b == false {
		w.Write(returnResult(false,"del error"))
	}else{
		w.Write(returnResult(true,"success"))
	}
}

func history_start_controller(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	id := GetFormInt(req,"id")
	if id == 0{
		w.Write(returnResult(false,"id error not be int"))
	}

	err := history.Start(dbname,id)
	if err != nil{
		w.Write(returnResult(false,err.Error()))
	}else{
		w.Write(returnResult(true,"success"))
	}
}

func history_kill_controller(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	id := GetFormInt(req,"id")
	if id == 0{
		w.Write(returnResult(false,"id error not be int"))
	}
	err := history.KillHistory(dbname,id)
	if err != nil{
		w.Write(returnResult(false,err.Error()))
	}else{
		w.Write(returnResult(true,"success"))
	}
}