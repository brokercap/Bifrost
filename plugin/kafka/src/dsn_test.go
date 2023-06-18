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

package src

import "testing"

func TestParseDSN(t *testing.T) {
	var url string

	url = "127.0.0.1:9092"
	p := ParseDSN(url)
	if len(p) != 1 {
		t.Fatalf("len(p) != 1")
	}
	if p["addr"] != url {
		t.Fatalf("addr(%s) != %s", p["addr"], url)
	}

	url = "127.0.0.1:9092,10.10.10.10"
	p = ParseDSN(url)
	if len(p) != 1 {
		t.Fatalf("len(p) != 1")
	}
	if p["addr"] != url {
		t.Fatalf("addr(%s) != %s", p["addr"], url)
	}

	url = "127.0.0.1:9092,10.10.10.10?from.beginning=false"
	p = ParseDSN(url)
	if len(p) != 2 {
		t.Fatalf("len(p) != 2")
	}

	url = "127.0.0.1:9092?from.beginning=true"
	p = ParseDSN(url)
	if len(p) != 2 {
		t.Fatalf("len(p) != 2 (%s)", p)
	}

}
