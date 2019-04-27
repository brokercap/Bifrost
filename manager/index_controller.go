package manager

import (
	"net/http"
	"html/template"
	"github.com/jc3wish/Bifrost/server"
	"github.com/jc3wish/Bifrost/plugin/driver"
	pluginStorage "github.com/jc3wish/Bifrost/plugin/storage"
	"encoding/json"
	"runtime"
	"github.com/jc3wish/Bifrost/config"
	"time"
)

var StartTime = ""

func init()  {

	StartTime = time.Now().Format("2006-01-03 15:04:05")

	addRoute("/",index_controller)
	addRoute("/overview",overview_controller)
	addRoute("/serverinfo",server_monitor_controller)


}

func index_controller(w http.ResponseWriter,req *http.Request){
	Index := TemplateHeader{Title:"Bifrost-Index"}
	t, _ := template.ParseFiles(TemplatePath("manager/template/index.html"),TemplatePath("manager/template/header.html"),TemplatePath("manager/template/footer.html"))
	t.Execute(w, Index)
}

func overview_controller(w http.ResponseWriter,req *http.Request){
	type OverView struct {
		DbCount 				int
		ToServerCount 			int
		PluginCount 			int
		TableCount				int
		GoVersion       		string
		BifrostVersion  		string
		BifrostPluginVersion 	string
		StartTime 				string
		GOOS					string
		GOARCH					string
	}
	var data OverView

	dbList := server.GetListDb()
	DbCount := len(dbList)

	TableCount := 0
	for _,v := range dbList{
		TableCount += v.TableCount
	}

	PluginCount := len(driver.Drivers())

	ToServerCount := len(pluginStorage.GetToServerMap())

	data = OverView{
		DbCount:				DbCount,
		ToServerCount:			ToServerCount,
		PluginCount:			PluginCount,
		TableCount:				TableCount,
		GoVersion:				runtime.Version(),
		BifrostVersion:			config.VERSION,
		BifrostPluginVersion:	driver.GetApiVersion(),
		StartTime:				StartTime,
		GOOS:					runtime.GOOS,
		GOARCH:					runtime.GOARCH,
	}
	b,_:=json.Marshal(data)
	w.Write(b)
}

func server_monitor_controller(w http.ResponseWriter,req *http.Request){
	type ServerMonitor struct {
		SeftMemStats 		*runtime.MemStats
	}
	memStat := new(runtime.MemStats)
	runtime.ReadMemStats(memStat)
	data := ServerMonitor{
		SeftMemStats:memStat,
	}
	b,_:=json.Marshal(data)
	w.Write(b)
}
