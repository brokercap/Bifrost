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
package router

import "github.com/brokercap/Bifrost/admin/xgo"
import "github.com/brokercap/Bifrost/admin/controller"

func init() {

	// index
	xgo.Router("/", &controller.IndexController{}, "*:Index")
	xgo.Router("/overview", &controller.IndexController{}, "*:Overview")
	xgo.Router("/serverMonitor", &controller.IndexController{}, "*:ServerMonitor")
	xgo.Router("/freeOSMemory", &controller.IndexController{}, "*:FreeOSMemory")

	// pprof
	xgo.Router("/debug/pprof/", &controller.PprofController{}, "*:Default")
	xgo.Router("/debug/pprof/allocs", &controller.PprofController{}, "*:Default")
	xgo.Router("/debug/pprof/block", &controller.PprofController{}, "*:Default")
	xgo.Router("/debug/pprof/heap", &controller.PprofController{}, "*:Default")
	xgo.Router("/debug/pprof/goroutine", &controller.PprofController{}, "*:Default")
	xgo.Router("/debug/pprof/threadcreate", &controller.PprofController{}, "*:Default")
	xgo.Router("/debug/pprof/mutex", &controller.PprofController{}, "*:Default")
	xgo.Router("/debug/pprof/cmdline", &controller.PprofController{}, "*:Cmdline")
	xgo.Router("/debug/pprof/profile", &controller.PprofController{}, "*:Profile")
	xgo.Router("/debug/pprof/symbol", &controller.PprofController{}, "*:Symbol")
	xgo.Router("/debug/pprof/trace", &controller.PprofController{}, "*:Trace")

	//db
	xgo.Router("/db/index", &controller.DBController{}, "*:Index")
	xgo.Router("/db/add", &controller.DBController{}, "POST,PUT:Add")
	xgo.Router("/db/update", &controller.DBController{}, "POST:Update")
	xgo.Router("/db/stop", &controller.DBController{}, "POST:Stop")
	xgo.Router("/db/start", &controller.DBController{}, "POST:Start")
	xgo.Router("/db/close", &controller.DBController{}, "POST:Close")
	xgo.Router("/db/del", &controller.DBController{}, "POST,DELETE:Delete")
	xgo.Router("/db/list", &controller.DBController{}, "*:List")
	xgo.Router("/db/check_uri", &controller.DBController{}, "*:CheckUri")
	xgo.Router("/db/get_last_position", &controller.DBController{}, "*:GetLastPosition")

	xgo.Router("/db/detail", &controller.DBController{}, "*:Detail")
	xgo.Router("/db/table/fields", &controller.DBController{}, "*:GetTableFields")
	xgo.Router("/db/table/list", &controller.DBController{}, "*:TableList")
	xgo.Router("/db/table/createsql", &controller.DBController{}, "*:ShowCreateSQL")
	xgo.Router("/db/version/get", &controller.DBController{}, "*:GetVersion")

	//backup
	xgo.Router("/backup/export", &controller.BackupController{}, "*:Export")
	xgo.Router("/backup/import", &controller.BackupController{}, "POST:Import")

	//channel
	xgo.Router("/channel/index", &controller.ChannelController{}, "*:Index")
	xgo.Router("/channel/list", &controller.ChannelController{}, "*:List")
	xgo.Router("/channel/add", &controller.ChannelController{}, "POST,PUT:Add")
	xgo.Router("/channel/stop", &controller.ChannelController{}, "POST:Stop")
	xgo.Router("/channel/start", &controller.ChannelController{}, "POST:Start")
	xgo.Router("/channel/del", &controller.ChannelController{}, "POST,DELETE:Delete")
	xgo.Router("/channel/close", &controller.ChannelController{}, "POST:Close")

	//chanel table bind
	xgo.Router("/channel/table/index", &controller.ChannelController{}, "*:TableListIndex")
	xgo.Router("/channel/table/list", &controller.ChannelController{}, "*:TableList")

	//docs
	xgo.Router("/docs", &controller.DocsController{}, "*:Index")
	xgo.Router("/api/docs", &controller.DocsController{}, "*:ApiDocIndex")

	//flow
	xgo.Router("/flow/index", &controller.FlowController{}, "*:Index")
	xgo.Router("/flow/get", &controller.FlowController{}, "*:GetFlow")

	//history
	xgo.Router("/history/index", &controller.HistoryController{}, "*:Index")
	xgo.Router("/history/list", &controller.HistoryController{}, "*:List")
	xgo.Router("/history/add", &controller.HistoryController{}, "POST,PUT:Add")
	xgo.Router("/history/del", &controller.HistoryController{}, "POST,DELETE:Delete")
	xgo.Router("/history/start", &controller.HistoryController{}, "POST:Start")
	xgo.Router("/history/stop", &controller.HistoryController{}, "POST:Stop")
	xgo.Router("/history/kill", &controller.HistoryController{}, "POST:Kill")
	xgo.Router("/history/check_where", &controller.HistoryController{}, "POST:CheckWhere")

	//user
	xgo.Router("/user/index", &controller.UserController{}, "*:Index")
	xgo.Router("/user/list", &controller.UserController{}, "*:List")
	xgo.Router("/user/update", &controller.UserController{}, "POST:Update")
	xgo.Router("/user/del", &controller.UserController{}, "POST,DELETE:Delete")
	xgo.Router("/user/login/log", &controller.UserController{}, "*:LastLoginLog")

	//login
	xgo.Router("/login/index", &controller.LoginController{}, "*:Index")
	xgo.Router("/dologin", &controller.LoginController{}, "POST:Login")
	xgo.Router("/logout", &controller.LoginController{}, "*:Logout")

	//table
	xgo.Router("/table/list", &controller.TableController{}, "*:List")
	xgo.Router("/table/add", &controller.TableController{}, "POST,PUT:Add")
	xgo.Router("/table/update", &controller.TableController{}, "POST:Update")
	xgo.Router("/table/del", &controller.TableController{}, "POST,DELETE:Delete")

	//table toserver bind
	xgo.Router("/table/toserver/list", &controller.TableToServerController{}, "*:List")
	xgo.Router("/table/toserver/add", &controller.TableToServerController{}, "POST,PUT:Add")
	xgo.Router("/table/toserver/start", &controller.TableToServerController{}, "POST:Start")
	xgo.Router("/table/toserver/stop", &controller.TableToServerController{}, "POST:Stop")
	xgo.Router("/table/toserver/deal", &controller.TableToServerController{}, "POST:DealError")
	xgo.Router("/table/toserver/del", &controller.TableToServerController{}, "POST,DELETE:Delete")

	//table sync
	xgo.Router("/table/synclist/index", &controller.TableSyncController{}, "*:Index")
	xgo.Router("/table/synclist/list", &controller.TableSyncController{}, "*:List")

	//toserver
	xgo.Router("/toserver/index", &controller.ToServerController{}, "*:Index")
	xgo.Router("/toserver/list", &controller.ToServerController{}, "*:List")
	xgo.Router("/toserver/add", &controller.ToServerController{}, "POST,PUT:Add")
	xgo.Router("/toserver/update", &controller.ToServerController{}, "POST:Update")
	xgo.Router("/toserver/del", &controller.ToServerController{}, "POST,DELETE:Delete")
	xgo.Router("/toserver/check_uri", &controller.ToServerController{}, "POST:CheckUri")

	//warning
	xgo.Router("/warning/config/index", &controller.WarningController{}, "*:Index")
	xgo.Router("/warning/config/list", &controller.WarningController{}, "*:List")
	xgo.Router("/warning/config/add", &controller.WarningController{}, "POST,PUT:Add")
	xgo.Router("/warning/config/del", &controller.WarningController{}, "POST,DELETE:Delete")
	xgo.Router("/warning/config/check", &controller.WarningController{}, "POST:Check")

	//file queue
	xgo.Router("/table/toserver/filequeue/update", &controller.FileQueueController{}, "POST:Update")
	xgo.Router("/table/toserver/filequeue/getinfo", &controller.FileQueueController{}, "*:GetInfo")

	//plugin
	xgo.Router("/plugin/index", &controller.PluginController{}, "*:Index")
	xgo.Router("/plugin/list", &controller.PluginController{}, "*:List")
	xgo.Router("/plugin/reload", &controller.PluginController{}, "*:Reload")
	xgo.Router("/plugin/getSupportedOtherOutputTypeList", &controller.PluginController{}, "*:GetSupportedOtherOutputTypeList")

	//refuseip
	xgo.Router("/refuseip/index", &controller.RefuseIpController{}, "*:Index")
	xgo.Router("/refuseip/list", &controller.RefuseIpController{}, "*:List")
	xgo.Router("/refuseip/del", &controller.RefuseIpController{}, "POST,DELETE:Del")

	//input plugin
	xgo.Router("/plugin/input/list", &controller.InputController{}, "*:List")
}
