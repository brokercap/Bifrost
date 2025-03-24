package main

import (
	"encoding/json"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"io/ioutil"
	"log"
	"net/http"
)

var i int

func handel_data(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		check_uri()
		break
	case "POST":
		post(w, req)
		break
	default:
		log.Println("Methon:", req.Method, " not supported ")
		break
	}
	w.Write([]byte("success"))
}

func check_uri() {
	log.Println("check uri success")
	return
}

func post(w http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)
	var data pluginDriver.PluginDataType
	err = json.Unmarshal(body, &data)
	if err != nil {
		w.WriteHeader(501)
		log.Println("body err:", string(body))
		return
	}
	log.Println("i:", i, "body:", string(body))
	i++
	return
}

func main() {
	i = 1
	http.HandleFunc("/bifrost_http_api_test", handel_data)
	http.ListenAndServe("0.0.0.0:3332", nil)

}
