package manager

import (
	"github.com/brokercap/Bifrost/server"
	"net/http"
)

func init(){
	addRoute("/table/toserver/filequeue/update",table_toserver_filequeue_update_controller)
}

func table_toserver_filequeue_update_controller(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	tablename := req.Form.Get("table_name")
	schema := req.Form.Get("schema_name")
	schema = tansferSchemaName(schema)
	tablename = tansferTableName(tablename)
	ToServerID := GetFormInt(req,"to_server_id")
	index :=  GetFormInt(req,"index")
	var err error
	ToServerInfo := server.GetDBObj(dbname).GetTable(schema,tablename).ToServerList[index]
	if ToServerInfo.ToServerID == ToServerID{
		ToServerInfo.FileQueueStart()
	}
	if err != nil{
		w.Write(returnResult(false,err.Error()))
	}else{
		w.Write(returnResult(true,"success"))
	}
}

