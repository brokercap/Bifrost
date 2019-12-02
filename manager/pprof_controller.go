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
package manager
import (
	"net/http"
)

func init()  {
	addRoute("/debug/pprof/",pprof_default_controlle)
	addRoute("/debug/pprof/allocs",pprof_default_controlle)
	addRoute("/debug/pprof/block",pprof_default_controlle)
	addRoute("/debug/pprof/heap",pprof_default_controlle)
	addRoute("/debug/pprof/goroutine",pprof_default_controlle)
	addRoute("/debug/pprof/threadcreate",pprof_default_controlle)
	addRoute("/debug/pprof/mutex",pprof_default_controlle)
	addRoute("/debug/pprof/cmdline",pprof_cmdline_controlle)
	addRoute("/debug/pprof/profile",pprof_profile_controlle)
	addRoute("/debug/pprof/symbol",pprof_symbol_controlle)
	addRoute("/debug/pprof/trace",pprof_trace_controlle)
}

func pprof_default_controlle(w http.ResponseWriter,req *http.Request)  {
	Index(w,req)
	return
}

func pprof_cmdline_controlle(w http.ResponseWriter,req *http.Request)  {
	Cmdline(w,req)
	return
}

func pprof_profile_controlle(w http.ResponseWriter,req *http.Request)  {
	Profile(w,req)
	return
}

func pprof_symbol_controlle(w http.ResponseWriter,req *http.Request)  {
	Symbol(w,req)
	return
}

func pprof_trace_controlle(w http.ResponseWriter,req *http.Request)  {
	Trace(w,req)
	return
}