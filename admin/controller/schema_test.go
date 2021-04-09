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
package controller

import "testing"

func TestGetGrantsFor(t *testing.T) {
	uri := "xxtest:xxtest@tcp(192.168.0.114:3306)/test"
	sql, err := GetGrantsFor(DBConnect(uri))
	if err != nil {
		t.Fatal(err)
	}
	t.Log(sql)
}

func TestCheckUserSlavePrivilege(t *testing.T) {
	uri := "xxtest:xxtest@tcp(127.0.0.1:3306)/test"
	err := CheckUserSlavePrivilege(DBConnect(uri))
	if err != nil {
		t.Fatal(err)
	}
	t.Log("test success")
}
