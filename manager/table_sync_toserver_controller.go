package manager

import (
	"net/http"
	"github.com/jc3wish/Bifrost/server"
	"encoding/json"
	"strings"
	"html/template"
	"log"
)

func init(){
	addRoute("/synclist",table_synclist_controller)
}

type syncListStruct struct {
	DbName string `json:"DbName"`
	SchemaName string `json:"SchemaName"`
	TableName string `json:"TableName"`
	ToServerList []*server.ToServer `json:"ToServerList"`
}

func table_synclist_controller(w http.ResponseWriter,req *http.Request) {
	req.ParseForm()
	dbname := req.Form.Get("dbname")
	tablename := req.Form.Get("table_name")
	schema := req.Form.Get("schema_name")
	var syncList []syncListStruct
	if tablename != "" {
		syncList = get_syncList_by_table_name(dbname,schema,tablename)
	}else if schema != ""{
		syncList = get_syncList_by_schema_name(dbname,schema)
	}else if dbname != ""{
		syncList = get_syncList_by_dbname(dbname)
	}else{
		syncList = get_syncList_all()
	}

	//假如传了 toserverkey 参数，则将 非 toserverkey 的列表给过滤掉
	toserverkey := req.Form.Get("toserverkey")
	if toserverkey != ""{
		var filterToServerKeySyncList []syncListStruct
		filterToServerKeySyncList = make([]syncListStruct,0)
		for _,v := range syncList{
			var b bool = false
			var tmp syncListStruct
			for _,val := range v.ToServerList{
				if val.ToServerKey == toserverkey{
					if b == false{
						b = true
						tmp = syncListStruct{
							DbName:v.DbName,
							SchemaName:v.SchemaName,
							TableName:v.TableName,
							ToServerList:make([]*server.ToServer,0),
						}
					}
					tmp.ToServerList = append(tmp.ToServerList,val)
				}
			}
			if b {
				filterToServerKeySyncList = append(filterToServerKeySyncList,tmp)
			}
		}
		syncList = filterToServerKeySyncList
	}

	switch req.Form.Get("format") {
	case "json":
		b,_:=json.Marshal(syncList)
		w.Write(b)
		break
	default:
		type syncListResult struct {
			TemplateHeader
			DbName string
			SchemaName string
			TableName string
			ToServerKey string
			SyncList []syncListStruct
		}
		data := syncListResult{
			DbName:dbname,
			SchemaName:schema,
			TableName:tablename,
			ToServerKey:toserverkey,
			SyncList:syncList,
		}
		data.Title = "SyncList - Bifrost"
		t, err := template.ParseFiles(TemplatePath("manager/template/sync.list.html"),TemplatePath("manager/template/header.html"),TemplatePath("manager/template/footer.html"))
		if err != nil{
			log.Println(err)
		}
		t.Execute(w, data)
	}
	return
}

func get_syncList_all() []syncListStruct{
	syncList := make([]syncListStruct,0)
	DBList := server.GetListDb()
	for _,db := range DBList{
		t1:=server.GetDBObj(db.Name)
		for key,table := range t1.GetTables(){
			tmpArr := strings.Split(key,"-")
			syncList = append(syncList,syncListStruct{db.Name,tmpArr[0],tmpArr[1],table.ToServerList})
		}
	}
	return syncList
}


func get_syncList_by_dbname(dbname string) []syncListStruct{
	syncList := make([]syncListStruct,0)
	t1 := server.GetDBObj(dbname)
	if t1 != nil{
		for key,table := range t1.GetTables(){
			tmpArr := strings.Split(key,"-")
			syncList = append(syncList,syncListStruct{dbname,tmpArr[0],tmpArr[1],table.ToServerList})
		}
	}
	return syncList
}


func get_syncList_by_schema_name(dbname,schema_name string) []syncListStruct{
	syncList := make([]syncListStruct,0)
	t1 := server.GetDBObj(dbname)
	if t1 != nil{
		for key,table := range t1.GetTables(){
			tmpArr := strings.Split(key,"-")
			if tmpArr[0] == schema_name {
				syncList = append(syncList, syncListStruct{dbname, tmpArr[0], tmpArr[1], table.ToServerList})
			}
		}
	}
	return syncList
}

func get_syncList_by_table_name(dbname,schema_name,table_name string) []syncListStruct{
	syncList := make([]syncListStruct,0)
	t1:=server.GetDBObj(dbname)
	if t1 == nil{
		return syncList
	}
	table:=t1.GetTable(schema_name,table_name)
	if table == nil{
		return syncList
	}
	syncList = append(syncList, syncListStruct{dbname, schema_name, table_name, table.ToServerList})
	return syncList
}
