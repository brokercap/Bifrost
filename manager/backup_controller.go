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
	"time"
	"github.com/brokercap/Bifrost/server"
	"io/ioutil"
)
func init(){
	addRoute("/backup/export",backup_export_controller)
	addRoute("/backup/import",backup_import_controller)
}

func backup_export_controller(w http.ResponseWriter,req *http.Request){
	b,err:=server.GetSnapshotData()
	if err != nil{
		w.Write(returnResult(false,err.Error()))
		return
	}
	fileName := "bifrost_"+time.Now().Format("2006-01-02 15:04:05")+".json"
	w.Header().Add("Content-Type", "application/octet-stream")
	w.Header().Add("content-disposition", "attachment; filename=\""+fileName+"\"")
	w.Write(b)
}

func backup_import_controller(w http.ResponseWriter,req *http.Request){
	req.ParseMultipartForm(32 << 20)
	file, _, err := req.FormFile("backup_file")
	if err != nil {
		w.Write(returnResult(false,err.Error()))
		return
	}
	fileContent, err := ioutil.ReadAll(file)
	file.Close()

	if err!=nil{
		w.Write(returnResult(false,err.Error()))
		return
	}

	server.DoRecoveryByBackupData(string(fileContent))

	w.Write(returnResult(true,"success"))

}