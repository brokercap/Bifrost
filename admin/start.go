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
package admin

import (
	"github.com/brokercap/Bifrost/admin/controller"
	_ "github.com/brokercap/Bifrost/admin/router"
	"github.com/brokercap/Bifrost/admin/xgo"
	"github.com/brokercap/Bifrost/config"
	"log"
	"runtime/debug"
)

func Start() {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
		}
	}()
	xgo.StartSession()
	xgo.AddStaticRoute("/css/", controller.AdminTemplatePath("/public/"))
	xgo.AddStaticRoute("/js/", controller.AdminTemplatePath("/public/"))
	xgo.AddStaticRoute("/fonts/", controller.AdminTemplatePath("/public/"))
	xgo.AddStaticRoute("/img/", controller.AdminTemplatePath("/public/"))
	xgo.AddStaticRoute("/plugin/", config.BifrostPluginTemplateDir)
	var err error
	if config.TLS {
		err = xgo.StartTLS(config.Listen, config.TLSServerKeyFile, config.TLSServerCrtFile)
	} else {
		err = xgo.Start(config.Listen)
	}
	if err != nil {
		log.Println("Manager Start Err:", err)
	}
}
