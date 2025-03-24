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
package http_manager

import "github.com/brokercap/Bifrost/admin/xgo"
import "github.com/brokercap/Bifrost/sdk/plugin/http_manager/controller"

func init() {

	//table toserver bind
	xgo.Router("/table/toserver/list", &controller.TableToServerController{}, "*:List")
	xgo.Router("/table/toserver/add", &controller.TableToServerController{}, "POST,PUT:Add")

	// index
	xgo.Router("/", &controller.IndexController{}, "*:Index")

	xgo.Router("/db/detail", &controller.DBController{}, "*:Detail")
	xgo.Router("/db/table/fields", &controller.DBController{}, "*:GetTableFields")
	xgo.Router("/db/table/list", &controller.DBController{}, "*:TableList")

	for k, _ := range controller.StaticMap {
		xgo.Router(k, &controller.OtherController{}, "*:StaticHtml")
	}

	xgo.Router("/overview", &controller.OtherController{}, "*:NotSupported")
	xgo.Router("/serverMonitor", &controller.OtherController{}, "*:NotSupported")
	xgo.Router("/freeOSMemory", &controller.OtherController{}, "*:NotSupported")

	// pprof
	xgo.Router("/debug/pprof/", &controller.OtherController{}, "*:NotSupported")
	xgo.Router("/debug/pprof/allocs", &controller.OtherController{}, "*:NotSupported")
	xgo.Router("/debug/pprof/block", &controller.OtherController{}, "*:NotSupported")
	xgo.Router("/debug/pprof/heap", &controller.OtherController{}, "*:NotSupported")
	xgo.Router("/debug/pprof/goroutine", &controller.OtherController{}, "*:NotSupported")
	xgo.Router("/debug/pprof/threadcreate", &controller.OtherController{}, "*:NotSupported")
	xgo.Router("/debug/pprof/mutex", &controller.OtherController{}, "*:NotSupported")
	xgo.Router("/debug/pprof/cmdline", &controller.OtherController{}, "*:NotSupported")
	xgo.Router("/debug/pprof/profile", &controller.OtherController{}, "*:NotSupported")
	xgo.Router("/debug/pprof/symbol", &controller.OtherController{}, "*:NotSupported")
	xgo.Router("/debug/pprof/trace", &controller.OtherController{}, "*:NotSupported")

	//db
	xgo.Router("/db/index", &controller.DBController{}, "*:NotSupported")
	xgo.Router("/db/add", &controller.DBController{}, "POST,PUT:NotSupported")
	xgo.Router("/db/update", &controller.DBController{}, "POST:NotSupported")
	xgo.Router("/db/stop", &controller.DBController{}, "POST:NotSupported")
	xgo.Router("/db/start", &controller.DBController{}, "POST:NotSupported")
	xgo.Router("/db/close", &controller.DBController{}, "POST:NotSupported")
	xgo.Router("/db/del", &controller.DBController{}, "POST,DELETE:NotSupported")
	xgo.Router("/db/list", &controller.DBController{}, "*:NotSupported")
	xgo.Router("/db/check_uri", &controller.DBController{}, "*:NotSupported")
	xgo.Router("/db/get_last_position", &controller.DBController{}, "*:NotSupported")

	xgo.Router("/db/table/createsql", &controller.DBController{}, "*:NotSupported")

	//backup
	xgo.Router("/backup/export", &controller.OtherController{}, "*:NotSupported")
	xgo.Router("/backup/import", &controller.OtherController{}, "POST:NotSupported")

	//channel
	xgo.Router("/channel/index", &controller.OtherController{}, "*:NotSupported")
	xgo.Router("/channel/list", &controller.OtherController{}, "*:NotSupported")
	xgo.Router("/channel/add", &controller.OtherController{}, "POST,PUT:NotSupported")
	xgo.Router("/channel/stop", &controller.OtherController{}, "POST:NotSupported")
	xgo.Router("/channel/start", &controller.OtherController{}, "POST:NotSupported")
	xgo.Router("/channel/del", &controller.OtherController{}, "POST,DELETE:NotSupported")
	xgo.Router("/channel/close", &controller.OtherController{}, "POST:NotSupported")

	//chanel table bind
	xgo.Router("/channel/table/index", &controller.OtherController{}, "*:NotSupported")
	xgo.Router("/channel/table/list", &controller.OtherController{}, "*:NotSupported")

	//docs
	xgo.Router("/docs", &controller.OtherController{}, "*:NotSupported")

	//flow
	xgo.Router("/flow/index", &controller.OtherController{}, "*:NotSupported")
	xgo.Router("/flow/get", &controller.OtherController{}, "*:NotSupported")

	//history
	xgo.Router("/history/index", &controller.OtherController{}, "*:NotSupported")
	xgo.Router("/history/list", &controller.OtherController{}, "*:NotSupported")
	xgo.Router("/history/add", &controller.OtherController{}, "POST,PUT:NotSupported")
	xgo.Router("/history/del", &controller.OtherController{}, "POST,DELETE:NotSupported")
	xgo.Router("/history/start", &controller.OtherController{}, "POST:NotSupported")
	xgo.Router("/history/stop", &controller.OtherController{}, "POST:NotSupported")
	xgo.Router("/history/kill", &controller.OtherController{}, "POST:NotSupported")
	xgo.Router("/history/check_where", &controller.OtherController{}, "POST:NotSupported")

	//user
	xgo.Router("/user/index", &controller.OtherController{}, "*:NotSupported")
	xgo.Router("/user/list", &controller.OtherController{}, "*:NotSupported")
	xgo.Router("/user/update", &controller.OtherController{}, "POST:NotSupported")
	xgo.Router("/user/del", &controller.OtherController{}, "POST,DELETE:NotSupported")

	//login
	xgo.Router("/login/index", &controller.OtherController{}, "*:NotSupported")
	xgo.Router("/dologin", &controller.OtherController{}, "POST:NotSupported")
	xgo.Router("/logout", &controller.OtherController{}, "*:NotSupported")

	//table
	xgo.Router("/table/list", &controller.OtherController{}, "*:NotSupported")
	xgo.Router("/table/add", &controller.OtherController{}, "POST,PUT:NotSupported")
	xgo.Router("/table/update", &controller.OtherController{}, "POST:NotSupported")
	xgo.Router("/table/del", &controller.OtherController{}, "POST,DELETE:NotSupported")

	xgo.Router("/table/toserver/start", &controller.TableToServerController{}, "POST:NotSupported")
	xgo.Router("/table/toserver/stop", &controller.TableToServerController{}, "POST:NotSupported")
	xgo.Router("/table/toserver/deal", &controller.TableToServerController{}, "POST:NotSupported")
	xgo.Router("/table/toserver/del", &controller.TableToServerController{}, "POST,DELETE:NotSupported")

	//table sync
	xgo.Router("/table/synclist/index", &controller.OtherController{}, "*:NotSupported")
	xgo.Router("/table/synclist/list", &controller.OtherController{}, "*:NotSupported")

	//toserver
	xgo.Router("/toserver/index", &controller.OtherController{}, "*:NotSupported")
	xgo.Router("/toserver/list", &controller.OtherController{}, "*:NotSupported")
	xgo.Router("/toserver/add", &controller.OtherController{}, "POST,PUT:NotSupported")
	xgo.Router("/toserver/update", &controller.OtherController{}, "POST:NotSupported")
	xgo.Router("/toserver/del", &controller.OtherController{}, "POST,DELETE:NotSupported")
	xgo.Router("/toserver/check_uri", &controller.OtherController{}, "POST:NotSupported")

	//warning
	xgo.Router("/warning/config/index", &controller.OtherController{}, "*:NotSupported")
	xgo.Router("/warning/config/list", &controller.OtherController{}, "*:NotSupported")
	xgo.Router("/warning/config/add", &controller.OtherController{}, "POST,PUT:NotSupported")
	xgo.Router("/warning/config/del", &controller.OtherController{}, "POST,DELETE:NotSupported")
	xgo.Router("/warning/config/check", &controller.OtherController{}, "POST:NotSupported")

	//file queue
	xgo.Router("/table/toserver/filequeue/update", &controller.OtherController{}, "POST:NotSupported")
	xgo.Router("/table/toserver/filequeue/getinfo", &controller.OtherController{}, "*:NotSupported")

	//plugin
	xgo.Router("/plugin/index", &controller.OtherController{}, "*:NotSupported")
	xgo.Router("/plugin/list", &controller.OtherController{}, "*:NotSupported")
	xgo.Router("/plugin/reload", &controller.OtherController{}, "*:NotSupported")

}
