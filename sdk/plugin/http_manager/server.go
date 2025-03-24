package http_manager

import (
	"github.com/brokercap/Bifrost/admin/xgo"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"strings"
)

const VERSION = "v1.6.0-plugin_dev_test"

type Param struct {
	Listen  string
	HtmlDir string
}

var pluginHtmlDir string = ""

func Start(p *Param) {
	if p.Listen == "" {
		p.Listen = "0.0.0.0:21066"
	}

	if p.HtmlDir == "" {
		panic("HtmlDir not be empty")
	}

	if _, err := os.Stat(p.HtmlDir); err != nil {
		log.Fatal(err)
	}

	pluginHtmlDir = p.HtmlDir

	http.HandleFunc("/plugin/", pluginHtml)
	xgo.StartSession()
	log.Println("http listen:", p.Listen)
	err := xgo.Start(p.Listen)
	log.Fatal(err)
}

func pluginHtml(w http.ResponseWriter, req *http.Request) {
	var fileDir string
	i := strings.LastIndex(req.RequestURI, "/www/")

	fileDir = strings.TrimSpace(req.RequestURI[i+5:])

	i = strings.IndexAny(fileDir, "?")
	if i > 0 {
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

	f, err := os.Open(pluginHtmlDir + "/" + fileDir)
	if err != nil {
		log.Println(pluginHtmlDir+"/"+fileDir, " not exsit", err)
		w.WriteHeader(404)
		return
	}

	defer f.Close()
	b, err := ioutil.ReadAll(f)
	if err != nil {
		log.Println("err:", err)
	}
	w.Write(b)
}
