package manager

import (
	"net/http"
	"html/template"
)
func init()  {
	addRoute("/",index_controller)
}

func index_controller(w http.ResponseWriter,req *http.Request){
	Index := TemplateHeader{Title:"Bifrost-Index"}
	t, _ := template.ParseFiles(TemplatePath("manager/template/index.html"),TemplatePath("manager/template/header.html"),TemplatePath("manager/template/footer.html"))
	t.Execute(w, Index)
}
