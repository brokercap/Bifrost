/*
Copyright [2018] [jc3wish]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package xgo

import (
	"net/http"
	"strings"
	"runtime/debug"
	"fmt"
	"log"
)

type HandlerFun interface {
	Callback(http.ResponseWriter,*http.Request)
}

type HandlerFunc func(http.ResponseWriter,*http.Request)

type FirstCallBackFun func(http.ResponseWriter,*http.Request) bool

func (f HandlerFunc) Callback(w http.ResponseWriter,r *http.Request) {
	f(w, r)
}

var routeMap map[string]HandlerFun
var FirstCallBack FirstCallBackFun

func init()  {
	routeMap = make(map[string]HandlerFun,0)
}

func SetFirstCallBack(callbackFUns func(http.ResponseWriter,*http.Request) bool){
	if FirstCallBack == nil{
		FirstCallBack = callbackFUns
	}
}

func AddRoute(route string, callbackFUns func(http.ResponseWriter,*http.Request) ) error{
	if _,ok:=routeMap[route];ok{
		return fmt.Errorf(route+" is exsit")
	}
	routeMap[route]=HandlerFunc(callbackFUns)
	http.HandleFunc(route,rounteFunc)
	return nil
}

func rounteFunc(w http.ResponseWriter,req *http.Request){
	defer func() {
		if err := recover();err!=nil{
			log.Println("err:",err,string(debug.Stack()))
		}
	}()
	var route string
	i := strings.IndexAny(req.RequestURI, "?")
	if i>0{
		route = strings.TrimSpace(req.RequestURI[0:i])
	}else{
		route = req.RequestURI
	}
	if _,ok:=routeMap[route];ok{
		if FirstCallBack(w,req) == true{
			routeMap[route].Callback(w,req)
		}
	}
}

func AddStaticRoute(route string, dir string ){
	http.Handle(route, http.FileServer(http.Dir(dir)))
}

func Start(IpAndPort string) error{
	return http.ListenAndServe(IpAndPort, nil)
}

func StartTLS(IpAndPort string,serverKey string,serverCrt string) error{
	return http.ListenAndServeTLS(IpAndPort, serverCrt,serverKey,nil)
}