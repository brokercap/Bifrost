package http_manager

import (
	"net/http"
	"encoding/json"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	toserver "github.com/brokercap/Bifrost/plugin/storage"

	"html/template"
)

type TemplateHeader struct {
	Title string
}


func db_detail_controller(w http.ResponseWriter,req *http.Request){
	type dbDetail struct {
		TemplateHeader
		DbName string
		DataBaseList []string
		ToServerList  map[string]*toserver.ToServer
		ChannelList map[int]interface{}
	}
	req.ParseForm()
	dbname := req.Form.Get("dbname")

	DataBaseList := []string{"bifrost_test"}
	var Result dbDetail
	Result = dbDetail{DataBaseList:DataBaseList,DbName:dbname,ToServerList: toserver.GetToServerMap(),ChannelList:make(map[int]interface{},0)}
	Result.Title = dbname + " - Detail - Bifrost"

	t := template.New("status")
	t, _ = t.Parse(IndexHtml)
	//data := map[string]interface{}{"html": IndexHtml}
	t.Execute(w,Result)
	return
}

func get_table_List_controller(w http.ResponseWriter,req *http.Request){
	type ResultType struct{
		TableName string
		ChannelName string
		AddStatus bool
	}
	var data []ResultType
	data = make([]ResultType,0)
	TableList := []string{"binlog_field_test"}
	for _,tableName := range TableList{
		data = append(data,ResultType{TableName:tableName,ChannelName:"default",AddStatus:true})
	}
	b,_:=json.Marshal(data)
	w.Write(b)
}


func get_table_fields_controller(w http.ResponseWriter,req *http.Request){
	b := `[{"COLUMN_NAME":"id","COLUMN_DEFAULT":"NULL","IS_NULLABLE":"NO","COLUMN_TYPE":"int(11) unsigned","COLUMN_KEY":"PRI","EXTRA":"auto_increment","COLUMN_COMMENT":"","DATA_TYPE":"int","NUMERIC_PRECISION":"10","NUMERIC_SCALE":"0"},{"COLUMN_NAME":"testtinyint","COLUMN_DEFAULT":"-1","IS_NULLABLE":"NO","COLUMN_TYPE":"tinyint(4)","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"tinyint","NUMERIC_PRECISION":"3","NUMERIC_SCALE":"0"},{"COLUMN_NAME":"testsmallint","COLUMN_DEFAULT":"-2","IS_NULLABLE":"NO","COLUMN_TYPE":"smallint(6)","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"smallint","NUMERIC_PRECISION":"5","NUMERIC_SCALE":"0"},{"COLUMN_NAME":"testmediumint","COLUMN_DEFAULT":"-3","IS_NULLABLE":"NO","COLUMN_TYPE":"mediumint(8)","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"mediumint","NUMERIC_PRECISION":"7","NUMERIC_SCALE":"0"},{"COLUMN_NAME":"testint","COLUMN_DEFAULT":"-4","IS_NULLABLE":"NO","COLUMN_TYPE":"int(11)","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"int","NUMERIC_PRECISION":"10","NUMERIC_SCALE":"0"},{"COLUMN_NAME":"testbigint","COLUMN_DEFAULT":"-5","IS_NULLABLE":"NO","COLUMN_TYPE":"bigint(20)","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"bigint","NUMERIC_PRECISION":"19","NUMERIC_SCALE":"0"},{"COLUMN_NAME":"testvarchar","COLUMN_DEFAULT":"NULL","IS_NULLABLE":"NO","COLUMN_TYPE":"varchar(10)","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"varchar","NUMERIC_PRECISION":"NULL","NUMERIC_SCALE":"NULL"},{"COLUMN_NAME":"testchar","COLUMN_DEFAULT":"NULL","IS_NULLABLE":"NO","COLUMN_TYPE":"char(2)","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"char","NUMERIC_PRECISION":"NULL","NUMERIC_SCALE":"NULL"},{"COLUMN_NAME":"testenum","COLUMN_DEFAULT":"en1","IS_NULLABLE":"NO","COLUMN_TYPE":"enum('en1','en2','en3')","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"enum","NUMERIC_PRECISION":"NULL","NUMERIC_SCALE":"NULL"},{"COLUMN_NAME":"testset","COLUMN_DEFAULT":"set1","IS_NULLABLE":"NO","COLUMN_TYPE":"set('set1','set2','set3')","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"set","NUMERIC_PRECISION":"NULL","NUMERIC_SCALE":"NULL"},{"COLUMN_NAME":"testtime","COLUMN_DEFAULT":"00:00:00","IS_NULLABLE":"NO","COLUMN_TYPE":"time","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"time","NUMERIC_PRECISION":"NULL","NUMERIC_SCALE":"NULL"},{"COLUMN_NAME":"testdate","COLUMN_DEFAULT":"0000-00-00","IS_NULLABLE":"NO","COLUMN_TYPE":"date","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"date","NUMERIC_PRECISION":"NULL","NUMERIC_SCALE":"NULL"},{"COLUMN_NAME":"testyear","COLUMN_DEFAULT":"1989","IS_NULLABLE":"NO","COLUMN_TYPE":"year(4)","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"year","NUMERIC_PRECISION":"NULL","NUMERIC_SCALE":"NULL"},{"COLUMN_NAME":"testtimestamp","COLUMN_DEFAULT":"CURRENT_TIMESTAMP","IS_NULLABLE":"NO","COLUMN_TYPE":"timestamp","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"timestamp","NUMERIC_PRECISION":"NULL","NUMERIC_SCALE":"NULL"},{"COLUMN_NAME":"testdatetime","COLUMN_DEFAULT":"0000-00-00 00:00:00","IS_NULLABLE":"NO","COLUMN_TYPE":"datetime","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"datetime","NUMERIC_PRECISION":"NULL","NUMERIC_SCALE":"NULL"},{"COLUMN_NAME":"testfloat","COLUMN_DEFAULT":"0.00","IS_NULLABLE":"NO","COLUMN_TYPE":"float(9,2)","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"float","NUMERIC_PRECISION":"9","NUMERIC_SCALE":"2"},{"COLUMN_NAME":"testdouble","COLUMN_DEFAULT":"0.00","IS_NULLABLE":"NO","COLUMN_TYPE":"double(9,2)","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"double","NUMERIC_PRECISION":"9","NUMERIC_SCALE":"2"},{"COLUMN_NAME":"testdecimal","COLUMN_DEFAULT":"0.00","IS_NULLABLE":"NO","COLUMN_TYPE":"decimal(9,2)","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"decimal","NUMERIC_PRECISION":"9","NUMERIC_SCALE":"2"},{"COLUMN_NAME":"testtext","COLUMN_DEFAULT":"NULL","IS_NULLABLE":"NO","COLUMN_TYPE":"text","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"text","NUMERIC_PRECISION":"NULL","NUMERIC_SCALE":"NULL"},{"COLUMN_NAME":"testblob","COLUMN_DEFAULT":"NULL","IS_NULLABLE":"NO","COLUMN_TYPE":"blob","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"blob","NUMERIC_PRECISION":"NULL","NUMERIC_SCALE":"NULL"},{"COLUMN_NAME":"testbit","COLUMN_DEFAULT":"b'0'","IS_NULLABLE":"NO","COLUMN_TYPE":"bit(8)","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"bit","NUMERIC_PRECISION":"8","NUMERIC_SCALE":"NULL"},{"COLUMN_NAME":"testbool","COLUMN_DEFAULT":"0","IS_NULLABLE":"NO","COLUMN_TYPE":"tinyint(1)","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"tinyint","NUMERIC_PRECISION":"3","NUMERIC_SCALE":"0"},{"COLUMN_NAME":"testmediumblob","COLUMN_DEFAULT":"NULL","IS_NULLABLE":"NO","COLUMN_TYPE":"mediumblob","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"mediumblob","NUMERIC_PRECISION":"NULL","NUMERIC_SCALE":"NULL"},{"COLUMN_NAME":"testlongblob","COLUMN_DEFAULT":"NULL","IS_NULLABLE":"NO","COLUMN_TYPE":"longblob","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"longblob","NUMERIC_PRECISION":"NULL","NUMERIC_SCALE":"NULL"},{"COLUMN_NAME":"testtinyblob","COLUMN_DEFAULT":"NULL","IS_NULLABLE":"NO","COLUMN_TYPE":"tinyblob","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"tinyblob","NUMERIC_PRECISION":"NULL","NUMERIC_SCALE":"NULL"},{"COLUMN_NAME":"test_unsinged_tinyint","COLUMN_DEFAULT":"1","IS_NULLABLE":"NO","COLUMN_TYPE":"tinyint(4) unsigned","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"tinyint","NUMERIC_PRECISION":"3","NUMERIC_SCALE":"0"},{"COLUMN_NAME":"test_unsinged_smallint","COLUMN_DEFAULT":"2","IS_NULLABLE":"NO","COLUMN_TYPE":"smallint(6) unsigned","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"smallint","NUMERIC_PRECISION":"5","NUMERIC_SCALE":"0"},{"COLUMN_NAME":"test_unsinged_mediumint","COLUMN_DEFAULT":"3","IS_NULLABLE":"NO","COLUMN_TYPE":"mediumint(8) unsigned","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"mediumint","NUMERIC_PRECISION":"7","NUMERIC_SCALE":"0"},{"COLUMN_NAME":"test_unsinged_int","COLUMN_DEFAULT":"4","IS_NULLABLE":"NO","COLUMN_TYPE":"int(11) unsigned","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"int","NUMERIC_PRECISION":"10","NUMERIC_SCALE":"0"},{"COLUMN_NAME":"test_unsinged_bigint","COLUMN_DEFAULT":"5","IS_NULLABLE":"NO","COLUMN_TYPE":"bigint(20) unsigned","COLUMN_KEY":"","EXTRA":"","COLUMN_COMMENT":"","DATA_TYPE":"bigint","NUMERIC_PRECISION":"20","NUMERIC_SCALE":"0"}]`
	w.Write([]byte(b))
}

func table_toserverlist_controller(w http.ResponseWriter,req *http.Request)  {
	w.Write([]byte("[]"))
}


func table_addToServer_controller(w http.ResponseWriter,req *http.Request){
	req.ParseForm()
	PluginName := req.Form.Get("plugin_name")
	toServerKey := req.Form.Get("toserver_key")
	p  := req.Form.Get("param")
	var pluginParam map[string]interface{}
	err := json.Unmarshal([]byte(p),&pluginParam)
	if err != nil{
		w.Write(returnResult(false,err.Error()))
		return
	}
	toServerInfo := toserver.GetToServerInfo(toServerKey)
	if toServerInfo == nil{
		w.Write(returnResult(false,toServerKey+" not exsit"))
		return
	}

	t := pluginDriver.Open(PluginName,toServerInfo.ConnUri)
	if t == nil{
		w.Write(returnResult(false,"plugin new error"))
		return
	}
	_,err = t.SetParam(pluginParam)
	if err != nil{
		w.Write(returnResult(false,err.Error()))
		return
	}

	w.Write(returnResult(false,"test success"))
}
