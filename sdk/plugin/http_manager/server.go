package http_manager

import (
	"github.com/jc3wish/Bifrost/manager/xgo"
	"os"
	"net/http"
	"strings"
	"log"
	"encoding/json"
	"path"
	"io/ioutil"
)

const VERSION  = "v1.1.0-plugin_dev_test"

var htmlMap map[string]string
func init()  {
	addRoute("/",index_controller)
}

type Param struct {
	Listen string
	HtmlDir string
}

func addRoute(route string, callbackFUns func(http.ResponseWriter,*http.Request) ){
	xgo.AddRoute(route,callbackFUns)
}

func controller_FirstCallback(w http.ResponseWriter,req *http.Request) bool {
	return true
}


func returnResult(r bool,msg string)[]byte{
	b,_:=json.Marshal(resultDataStruct{Status:r,Msg:msg})
	return  b
}

var pluginHtmlDir string = ""

func Start(p *Param)  {
	if p.Listen == ""{
		p.Listen = "0.0.0.0:21066"
	}

	if p.HtmlDir == ""{
		panic("HtmlDir not be empty")
	}

	if _, err := os.Stat(p.HtmlDir);err != nil{
		log.Fatal(err)
	}

	pluginHtmlDir = p.HtmlDir

	xgo.SetFirstCallBack(controller_FirstCallback)

	http.HandleFunc("/plugin/",pluginHtml)
	doInitManagerHttp()

	other()
	log.Println("http listen:",p.Listen)
	err := xgo.Start(p.Listen)
	log.Fatal(err)
}


func pluginHtml(w http.ResponseWriter,req *http.Request)  {
	var fileDir string
	i := strings.LastIndex(req.RequestURI, "/www/")

	fileDir = strings.TrimSpace(req.RequestURI[i+5:])

	i = strings.IndexAny(fileDir, "?")
	if i>0{
		fileDir = strings.TrimSpace(fileDir[0:i])
	}

	var filenameWithSuffix string
	filenameWithSuffix = path.Base(fileDir) //获取文件名带后缀
	var fileSuffix string
	fileSuffix = path.Ext(filenameWithSuffix) //获取文件后缀

	switch fileSuffix {
	case ".js":
		w.Header().Set("Content-Type", "application/javascript; charset=UTF-8")
		break
	case ".css":
		w.Header().Set("Content-Type", "text/css; charset=UTF-8")
		break
	default:
		break
	}

	f, err := os.Open(pluginHtmlDir+"/"+fileDir)
	if err != nil {
		log.Println(pluginHtmlDir+"/"+fileDir," not exsit",err)
		w.WriteHeader(404)
		return
	}

	defer f.Close()
	b,err := ioutil.ReadAll(f)
	if err!=nil{
		log.Println("err:",err)
	}
	w.Write(b)
}

func doInitManagerHttp()  {
	for k,_ := range htmlMap{
		addRoute(k,managerHttpFileController)
	}
}

func index_controller(w http.ResponseWriter,req *http.Request){
	http.Redirect(w, req, "/db/detail?dbname=mysqlTest", http.StatusFound)
}

func managerHttpFileController(w http.ResponseWriter,req *http.Request)  {
	var route string
	i := strings.IndexAny(req.RequestURI, "?")
	if i>0{
		route = strings.TrimSpace(req.RequestURI[0:i])
	}else{
		route = req.RequestURI
	}

	var filenameWithSuffix string
	filenameWithSuffix = path.Base(route) //获取文件名带后缀
	var fileSuffix string
	fileSuffix = path.Ext(filenameWithSuffix) //获取文件后缀

	switch fileSuffix {
	case ".js":
		w.Header().Set("Content-Type", "application/javascript; charset=UTF-8")
		break
	case ".css":
		w.Header().Set("Content-Type", "text/css; charset=UTF-8")
		break
	default:
		break
	}

	w.Write([]byte(htmlMap[route]))
}

type resultDataStruct struct {
	Status bool `json:"status"`
	Msg string `json:"msg"`
	Data interface{} `json:"data"`
}


func otherController(w http.ResponseWriter,req *http.Request)  {
	data := resultDataStruct{
		Status:false,
		Msg:"plugin dev,not supported",
		Data:"",
	}
	b,_:=json.Marshal(data)
	w.Write(b)
}

func other()  {

	addRoute("/channel/list",otherController)
	addRoute("/channel/add",otherController)
	addRoute("/channel/stop",otherController)
	addRoute("/channel/start",otherController)
	addRoute("/channel/del",otherController)
	addRoute("/channel/close",otherController)
	addRoute("/channel/tablelist",otherController)

	addRoute("/db/add",otherController)
	addRoute("/db/stop",otherController)
	addRoute("/db/start",otherController)
	addRoute("/db/close",otherController)
	addRoute("/db/del",otherController)
	addRoute("/db/list",otherController)
	addRoute("/db/check_uri",otherController)

	addRoute("/db/detail",db_detail_controller)
	addRoute("/db/tablelist",get_table_List_controller)
	addRoute("/db/tablefields",get_table_fields_controller)

	addRoute("/flow/get",otherController)
	addRoute("/flow/index",otherController)

	addRoute("/history/list",otherController)
	addRoute("/history/add",otherController)
	addRoute("/history/stop",otherController)
	addRoute("/history/del",otherController)
	addRoute("/history/start",otherController)

	addRoute("/overview",otherController)
	addRoute("/serverinfo",otherController)

	addRoute("/plugin/list",otherController)
	addRoute("/plugin/reload",otherController)

	addRoute("/table/del",otherController)
	addRoute("/table/add",otherController)
	addRoute("/table/toserverlist",table_toserverlist_controller)
	addRoute("/table/deltoserver",otherController)
	addRoute("/table/addtoserver",table_addToServer_controller)
	addRoute("/table/toserver/deal",otherController)

	addRoute("/synclist",otherController)

	addRoute("/toserver/add",otherController)
	addRoute("/toserver/update",otherController)
	addRoute("/toserver/del",otherController)
	addRoute("/toserver/list",otherController)
	addRoute("/toserver/check_uri",otherController)

	addRoute("/login",otherController)
	addRoute("/dologin",otherController)
	addRoute("/logout",otherController)

	addRoute("/getversion",get_version_controller)

	addRoute("/warning/config/list",otherController)
	addRoute("/warning/config/add",otherController)
	addRoute("/warning/config/del",otherController)
	addRoute("/warning/config/check",otherController)
}

func get_version_controller(w http.ResponseWriter,req *http.Request)  {
	w.Write([]byte(VERSION))
}