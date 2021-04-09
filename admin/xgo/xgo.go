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
	"log"
	"net/http"
	"reflect"
	"runtime/debug"
	"strings"
)

type routeController struct {
	controllerType reflect.Type
	controllerName string
	funName        string
	methodMap      map[string]bool
}

func NewRouteController(controllerType reflect.Type, controllerName string, funName string, methodMap map[string]bool) *routeController {
	return &routeController{
		controllerType: controllerType,
		controllerName: controllerName,
		funName:        funName,
		methodMap:      methodMap,
	}
}

func (route *routeController) AddMethod(methodName string) *routeController {
	if route.methodMap == nil {
		route.methodMap = make(map[string]bool, 0)
	}
	route.methodMap[strings.ToUpper(methodName)] = true
	return route
}

func (route *routeController) CheckMethod(methodName string) bool {
	if route.methodMap == nil {
		return false
	}
	var ok bool
	if _, ok = route.methodMap["*"]; ok {
		return true
	}
	if _, ok = route.methodMap[strings.ToUpper(methodName)]; ok {
		return true
	}
	return false
}

func (route *routeController) DoController(w http.ResponseWriter, req *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			if err == ErrAbort {
				return
			} else {
				log.Println("xgo doController:", err, string(debug.Stack()))
			}
		}
	}()
	if route.CheckMethod(req.Method) {
		route.DoController0(w, req)
	}
}

func (route *routeController) DoController0(w http.ResponseWriter, req *http.Request) {
	vc := reflect.New(route.controllerType)
	execController := vc.Interface().(ControllerInterface)
	execController.Init(&Context{Request: req, ResponseWriter: w, Session: sessionMgr}, route.controllerName, route.funName)
	execController.Prepare()
	t := reflect.ValueOf(execController)
	t.MethodByName(route.funName).Call(nil)
	execController.NormalStop()
}

var routeMap map[string]*routeController

func init() {
	routeMap = make(map[string]*routeController, 0)
}

func Router(route string, c ControllerInterface, FunNames string) error {
	var ok bool
	reflectVal := reflect.ValueOf(c)
	t := reflect.Indirect(reflectVal).Type()
	for _, FunName0 := range strings.Split(FunNames, ";") {
		if FunName0 == "" {
			continue
		}
		Arr := strings.Split(FunName0, ":")
		if len(Arr) == 0 {
			if _, ok = routeMap[route]; !ok {
				routeMap[route] = NewRouteController(t, t.Name(), Arr[0], make(map[string]bool, 0))
			}
			routeMap[route].AddMethod("*")
			continue
		}
		for _, method := range strings.Split(Arr[0], ",") {
			if _, ok = routeMap[route]; !ok {
				routeMap[route] = NewRouteController(t, t.Name(), Arr[1], make(map[string]bool, 0))
			}
			routeMap[route].AddMethod(method)
		}
	}
	http.HandleFunc(route, rounteFunc)
	return nil
}

func rounteFunc(w http.ResponseWriter, req *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("err:", err, string(debug.Stack()))
		}
	}()
	var route string
	i := strings.IndexAny(req.RequestURI, "?")
	if i > 0 {
		route = strings.TrimSpace(req.RequestURI[0:i])
	} else {
		route = req.RequestURI
	}
	var ok bool
	if _, ok = routeMap[route]; !ok {
		if strings.Index(route, "/favicon.ico") == -1 {
			log.Printf("route:%s 404", route)
		}
		return
	}
	routeMap[route].DoController(w, req)
}

func AddStaticRoute(route string, dir string) {
	http.Handle(route, http.FileServer(http.Dir(dir)))
}

func Start(IpAndPort string) error {
	return http.ListenAndServe(IpAndPort, nil)
}

func StartTLS(IpAndPort string, serverKey string, serverCrt string) error {
	return http.ListenAndServeTLS(IpAndPort, serverCrt, serverKey, nil)
}
