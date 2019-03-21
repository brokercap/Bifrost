package pluginTest

import (
	"os"
	"strconv"
	"net/http/cookiejar"
	"net/url"
	"bytes"
	"log"
	"github.com/jc3wish/Bifrost/Bristol/mysql"
	"database/sql/driver"
	"strings"
	"encoding/json"
	"net/http"
	"io/ioutil"
	"fmt"
)

type resultStruct struct {
	Status bool `json:"status"`
	Msg string `json:"msg"`
}

type MySQLConn struct {
	Uri string
	db mysql.MysqlConnection
}

type MasterBinlogInfoStruct struct {
	File string
	Position int
	Binlog_Do_DB string
	Binlog_Ignore_DB string
	Executed_Gtid_Set string
}


func(This *MySQLConn) DBConnect(){
	This.db = mysql.NewConnect(This.Uri)
}

func(This *MySQLConn) GetBinLogInfo() MasterBinlogInfoStruct{
	sql := "SHOW MASTER STATUS"
	stmt,err := This.db.Prepare(sql)
	if err !=nil{
		log.Println(err)
		return MasterBinlogInfoStruct{}
	}
	defer stmt.Close()
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	defer rows.Close()
	if err != nil {
		log.Printf("%v\n", err)
		return MasterBinlogInfoStruct{}
	}
	var File string
	var Position int
	var Binlog_Do_DB string
	var Binlog_Ignore_DB string
	var Executed_Gtid_Set string
	for {
		dest := make([]driver.Value, 4, 4)
		errs := rows.Next(dest)
		if errs != nil {
			return MasterBinlogInfoStruct{}
		}
		File = string(dest[0].([]byte))
		Binlog_Do_DB = string(dest[2].([]byte))
		Binlog_Ignore_DB = string(dest[3].([]byte))
		Executed_Gtid_Set = ""
		PositonString := string(dest[1].([]byte))
		Position,_ = strconv.Atoi(PositonString)
		break
	}

	return MasterBinlogInfoStruct{
		File:File,
		Position:Position,
		Binlog_Do_DB:Binlog_Do_DB,
		Binlog_Ignore_DB:Binlog_Ignore_DB,
		Executed_Gtid_Set:Executed_Gtid_Set,
	}

}

func(This *MySQLConn) GetServerId() int{
	sql := "show variables like 'server_id'"
	stmt,err := This.db.Prepare(sql)
	if err !=nil{
		log.Println(err)
		return 0
	}
	defer stmt.Close()
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	if err != nil {
		log.Printf("%v\n", err)
		return 0
	}
	defer rows.Close()
	var ServerId int
	for{
		dest := make([]driver.Value, 2, 2)
		errs := rows.Next(dest)
		if errs != nil{
			return 0
		}
		ServerIdString := string(dest[1].([]byte))
		ServerId,_ = strconv.Atoi(ServerIdString)
		break
	}
	return ServerId
}

func(This *MySQLConn) ExecSQL(sql string){
	p := make([]driver.Value, 0)
	_,err := This.db.Exec(sql,p)
	if err != nil{
		log.Println("ExecSQL:",sql," Err:",err)
	}else{
		log.Println("ExecSQL success:",sql)
	}
	return
}


type BifrostManager struct {
	Host string
	User string
	Pwd  string
	CurCookies []*http.Cookie
	CurCookieJar *cookiejar.Jar //管理cookie
	MysqlConn *MySQLConn
}


func(This *BifrostManager) Init(){
	This.CurCookies = nil
	This.CurCookieJar,_ = cookiejar.New(nil)
	This.DoLogin()
	log.Println(This.MysqlConn.Uri)
	This.MysqlConn.DBConnect()
}

func(This *BifrostManager) getUrlRespHtml(API string, postDict map[string]string) []byte{
	log.Println(API,"start","param:",postDict)
	strUrl := This.Host+API
	httpClient := &http.Client{
		Jar:This.CurCookieJar,
	}

	var httpReq *http.Request
	if nil == postDict {
		//log.Printf("is GET\n")
		httpReq, _ = http.NewRequest("GET", strUrl, nil)

	} else {
		//log.Printf("is POST\n")
		postValues := url.Values{}
		for postKey, PostValue := range postDict{
			postValues.Set(postKey, PostValue)
		}
		//log.Printf("postValues=%s\n", postValues)
		postDataStr := postValues.Encode()
		//log.Printf("postDataStr=%s\n", postDataStr)
		postDataBytes := []byte(postDataStr)
		//log.Printf("postDataBytes=%s\n", postDataBytes)
		postBytesReader := bytes.NewReader(postDataBytes)
		httpReq, _ = http.NewRequest("POST", strUrl, postBytesReader)
		httpReq.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	}

	httpResp, err := httpClient.Do(httpReq)
	if err != nil {
		log.Printf("http get strUrl=%s response error=%s\n", strUrl, err.Error())
		os.Exit(1)
	}
	//log.Printf("httpResp.Header=%s\n", httpResp.Header)
	//log.Printf("httpResp.Status=%s\n", httpResp.Status)

	defer httpResp.Body.Close()

	body, errReadAll := ioutil.ReadAll(httpResp.Body)
	if errReadAll != nil {
		log.Printf("get response for strUrl=%s got error=%s\n", strUrl, errReadAll.Error())
		os.Exit(1)
	}

	This.CurCookies = This.CurCookieJar.Cookies(httpReq.URL)
	log.Println(API,string(body),"param:",postDict)
	return body
}

func(This *BifrostManager) DoLogin() bool{
	p := make(map[string]string,0)
	p["user_name"] = This.User
	p["password"] = This.Pwd
	body := This.getUrlRespHtml("/dologin",p)

	var data resultStruct
	err2 := json.Unmarshal(body,&data)
	if err2 != nil{
		log.Println("login result err:",err2)
		os.Exit(1)
	}
	if data.Status != true{
		log.Println("login err:",data.Msg)
		os.Exit(1)
	}
	return true
}

func(This *BifrostManager) AddToServer(toServerKeyName string,pluginName string,uri string,notes string){
	postParam := make(map[string]string,0)
	postParam["toserverkey"] = toServerKeyName
	postParam["connuri"] = uri
	postParam["type"] = pluginName
	postParam["notes"] = notes
	This.getUrlRespHtml("/toserver/add",postParam)
}


func(This *BifrostManager) AddDB(dbname string,uri string){
	masterInfo := This.MysqlConn.GetBinLogInfo()
	if masterInfo.File == ""{
		log.Println(This.MysqlConn.Uri," not supported binlog")
		os.Exit(1)
	}
	serverId := This.MysqlConn.GetServerId()
	serverId = serverId+256
	postParam := make(map[string]string,0)
	postParam["dbname"] = dbname
	postParam["uri"] = This.MysqlConn.Uri
	postParam["filename"] = masterInfo.File
	postParam["position"] = strconv.Itoa(masterInfo.Position)
	postParam["serverid"] = strconv.Itoa(serverId)
	postParam["max_filename"] = ""
	postParam["max_position"] = ""
	This.getUrlRespHtml("/db/add",postParam)
}


func(This *BifrostManager)  AddTable(dbname string,schema string,table string,channelid string){
	postParam := make(map[string]string,0)
	postParam["dbname"] = dbname
	postParam["schema_name"] = schema
	postParam["table_name"] = table
	postParam["channelid"] = channelid
	This.getUrlRespHtml("/table/add",postParam)
}

func(This *BifrostManager)  DelTable(dbname string,schema string,table string){
	postParam := make(map[string]string,0)
	postParam["dbname"] = dbname
	postParam["schema_name"] = schema
	postParam["table_name"] = table
	This.getUrlRespHtml("/table/del",postParam)
}

func(This *BifrostManager)  AddTableToServer(dbname string,schema string,table string,toserver_key string,plugin_name string,fieldlist []string,mustbe string,param map[string]interface{}){
	postParam := make(map[string]string,0)
	postParam["dbname"] = dbname
	postParam["schema_name"] = schema
	postParam["table_name"] = table
	postParam["toserver_key"] = toserver_key
	postParam["plugin_name"] = plugin_name
	postParam["fieldlist"] = strings.Replace(strings.Trim(fmt.Sprint(fieldlist), "[]"), " ", ",", -1)
	postParam["mustbe"] = mustbe
	p,err := json.Marshal(param)
	if err!=nil{
		log.Println("addTableToServer err:",err)
		os.Exit(1)
	}
	postParam["param"] = string(p)
	This.getUrlRespHtml("/table/addtoserver",postParam)
}

func(This *BifrostManager)  DelTableToServer(dbname string,schema string,table string,toserver_key string,to_server_id string,index string){
	postParam := make(map[string]string,0)
	postParam["dbname"] = dbname
	postParam["schema_name"] = schema
	postParam["table_name"] = table
	postParam["toserver_key"] = toserver_key
	postParam["to_server_id"] = to_server_id
	postParam["index"] = index
	This.getUrlRespHtml("/table/deltoserver",postParam)
}


func(This *BifrostManager)  ChannelStart(dbname string,channelid string){
	postParam := make(map[string]string,0)
	postParam["dbname"] = dbname
	postParam["channelid"] = channelid
	This.getUrlRespHtml("/channel/start",postParam)
}

func(This *BifrostManager)  ChannelStop(dbname string,channelid string){
	postParam := make(map[string]string,0)
	postParam["dbname"] = dbname
	postParam["channelid"] = channelid
	This.getUrlRespHtml("/channel/stop",postParam)
}


func(This *BifrostManager)  ChannelClose(dbname string,channelid string){
	postParam := make(map[string]string,0)
	postParam["dbname"] = dbname
	postParam["channelid"] = channelid
	This.getUrlRespHtml("/channel/close",postParam)
}


func(This *BifrostManager)  ChannelDel(dbname string,channelid string){
	postParam := make(map[string]string,0)
	postParam["dbname"] = dbname
	postParam["channelid"] = channelid
	This.getUrlRespHtml("/channel/del",postParam)
}


func(This *BifrostManager)  DBStart(dbname string){
	postParam := make(map[string]string,0)
	postParam["dbname"] = dbname
	This.getUrlRespHtml("/db/start",postParam)
}

func(This *BifrostManager)  DBStop(dbname string){
	postParam := make(map[string]string,0)
	postParam["dbname"] = dbname
	This.getUrlRespHtml("/db/stop",postParam)
}

func(This *BifrostManager)  DBClose(dbname string){
	postParam := make(map[string]string,0)
	postParam["dbname"] = dbname
	This.getUrlRespHtml("/db/close",postParam)
}

func(This *BifrostManager)  DBDel(dbname string){
	postParam := make(map[string]string,0)
	postParam["dbname"] = dbname
	This.getUrlRespHtml("/db/del",postParam)
}

