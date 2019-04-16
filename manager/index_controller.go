package manager

import (
	"net/http"
	"html/template"
	"github.com/jc3wish/Bifrost/server"
	"github.com/jc3wish/Bifrost/plugin/driver"
	"github.com/jc3wish/Bifrost/plugin"
	"encoding/json"
)
func init()  {
	addRoute("/",index_controller)
	addRoute("/overview",overview_controller)
}

func index_controller(w http.ResponseWriter,req *http.Request){
	Index := TemplateHeader{Title:"Bifrost-Index"}
	t, _ := template.ParseFiles(TemplatePath("manager/template/index.html"),TemplatePath("manager/template/header.html"),TemplatePath("manager/template/footer.html"))
	t.Execute(w, Index)
}

func overview_controller(w http.ResponseWriter,req *http.Request){
	type OverView struct {
		DbCount 		int
		ToServerCount 	int
		PluginCount 	int
		TableCount		int
	}
	var data OverView

	dbList := server.GetListDb()
	DbCount := len(dbList)

	TableCount := 0
	for _,v := range dbList{
		TableCount += v.TableCount
	}

	PluginCount := len(driver.Drivers())

	ToServerCount := len(plugin.GetToServerMap())

	data = OverView{
		DbCount:DbCount,
		ToServerCount:ToServerCount,
		PluginCount:PluginCount,
		TableCount:TableCount,
	}
	b,_:=json.Marshal(data)
	w.Write(b)
}
