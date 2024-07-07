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

package mock

func (c *InputMock) AddReplicateDoDb(SchemaName, TableName string) (err error) {
	err = c.AddReplicateDoDb0(SchemaName, TableName)
	if err != nil {
		return err
	}
	c.StartTableWithName(SchemaName, TableName)
	return
}

func (c *InputMock) DelReplicateDoDb(SchemaName, TableName string) (err error) {
	err = c.DelReplicateDoDb0(SchemaName, TableName)
	if err != nil {
		return err
	}
	return
}
