package manager

import (
	"net/http"
	"html/template"
	"github.com/brokercap/Bifrost/server/history"
	"github.com/brokercap/Bifrost/server"
	"encoding/json"
	"log"
)
func init(){
	addRoute("/history/list",history_list_controller)
	addRoute("/history/add",history_add_controller)
	addRoute("/history/stop",history_kill_controller)
	addRoute("/history/del",history_del_controller)
	addRoute("/history/start",history_start_controller)
	addRoute("/history/checkwhere",history_check_where_controller)
}

func history_list_controller(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	tablename := req.Form.Get("table_name")
	schema := req.Form.Get("schema_name")
	var status history.HisotryStatus
	switch req.Form.Get("status") {
	case "close":
		status = history.HISTORY_STATUS_CLOSE
		break
	case "running":
		status = history.HISTORY_STATUS_RUNNING
		break
	case "selectOver":
		status = history.HISTORY_STATUS_SELECT_OVER
		break
	case "over":
		status = history.HISTORY_STATUS_OVER
		break
	case "halfway":
		status = history.HISTORY_STATUS_HALFWAY
		break
	case "killed":
		status = history.HISTORY_STATUS_KILLED
		break
	default:
		status = history.HISTORY_STATUS_ALL
		break
	}
	HistoryList := history.GetHistoryList(dbname,schema,tablename,status)

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
		DbList 			map[string]server.DbListStruct
		StatusList []history.HisotryStatus
		Status		history.HisotryStatus
	}
	var data HistoryListInfo
	data = HistoryListInfo{
		DbName:dbname,
		TableName:tablename,
		SchemaName:schema,
		HistoryList:HistoryList,
		DbList:server.GetListDb(),
		StatusList:[]history.HisotryStatus{
			history.HISTORY_STATUS_ALL,
			history.HISTORY_STATUS_CLOSE,
			history.HISTORY_STATUS_RUNNING,
			history.HISTORY_STATUS_HALFWAY,
			history.HISTORY_STATUS_SELECT_OVER,
			history.HISTORY_STATUS_OVER,
			history.HISTORY_STATUS_KILLED},
		Status:status,
	}

	data.Title = "History List - Bifrost"
	t, err := template.ParseFiles(TemplatePath("manager/template/history.list.html"),TemplatePath("manager/template/header.html"),TemplatePath("manager/template/footer.html"))
	if err != nil{
		log.Println(err)
	}
	t.Execute(w, data)
}


func history_add_controller(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	tablename := req.Form.Get("table_name")
	schema := req.Form.Get("schema_name")
	property := req.Form.Get("property")
	ToServerIDList := req.Form.Get("ToserverIds")
	if tansferTableName(tablename) == "*"{
		w.Write(returnDataResult(false,"不能给 AllTables 添加全量任务!",0))
		return
	}
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
	err = history.CheckWhere(dbname,schema,tablename,Property.Where)
	if err != nil{
		w.Write(returnDataResult(false,err.Error(),0))
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

func history_check_where_controller(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	tablename := req.Form.Get("table_name")
	schema := req.Form.Get("schema_name")
	property := req.Form.Get("property")
	var err error
	var Property history.HistoryProperty
	err = json.Unmarshal([]byte(property),&Property)
	if err != nil{
		w.Write(returnResult(false,err.Error()))
		return
	}
	if Property.Where == ""{
		w.Write(returnResult(true,"success"))
		return
	}
	err = history.CheckWhere(dbname,schema,tablename,Property.Where)
	if err != nil{
		w.Write(returnResult(false,err.Error()))
		return
	}
	w.Write(returnResult(true,"success"))
}