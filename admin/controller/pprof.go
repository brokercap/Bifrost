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

import "github.com/brokercap/Bifrost/admin/model"

type PprofController struct {
	CommonController
}

// 默认
func (c *DBController) Default() {
	c.SetOutputByUser()
	model.Index(c.Ctx.ResponseWriter, c.Ctx.Request)
}

// cmdline
func (c *DBController) Cmdline() {
	c.SetOutputByUser()
	model.Cmdline(c.Ctx.ResponseWriter, c.Ctx.Request)
}

// Profile
func (c *DBController) Profile() {
	c.SetOutputByUser()
	model.Profile(c.Ctx.ResponseWriter, c.Ctx.Request)
}

// Symbol
func (c *DBController) Symbol() {
	c.SetOutputByUser()
	model.Symbol(c.Ctx.ResponseWriter, c.Ctx.Request)
}

// Trace
func (c *DBController) Trace() {
	c.SetOutputByUser()
	model.Trace(c.Ctx.ResponseWriter, c.Ctx.Request)
}
