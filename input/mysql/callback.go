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
package mysql

import (
	"fmt"
	"github.com/brokercap/Bifrost/Bristol/mysql"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"strconv"
	"strings"
)

func evenTypeName(e mysql.EventType) string {
	switch e {
	case mysql.WRITE_ROWS_EVENTv0, mysql.WRITE_ROWS_EVENTv1, mysql.WRITE_ROWS_EVENTv2:
		return "insert"
	case mysql.UPDATE_ROWS_EVENTv0, mysql.UPDATE_ROWS_EVENTv1, mysql.UPDATE_ROWS_EVENTv2:
		return "update"
	case mysql.DELETE_ROWS_EVENTv0, mysql.DELETE_ROWS_EVENTv1, mysql.DELETE_ROWS_EVENTv2:
		return "delete"
	case mysql.QUERY_EVENT:
		return "sql"
	case mysql.XID_EVENT:
		return "commit"
	default:
		break
	}
	return fmt.Sprintf("%d", e)
}

func (c *MysqlInput) MySQLCallback(data *mysql.EventReslut) {
	if c.callback == nil {
		return
	}
	i := strings.IndexAny(data.BinlogFileName, ".")
	intString := data.BinlogFileName[i+1:]
	BinlogFileNum,_:=strconv.Atoi(intString)
	data0 := &pluginDriver.PluginDataType{
		Timestamp:data.Header.Timestamp,
		EventType:evenTypeName(data.Header.EventType),
		SchemaName:data.SchemaName,
		TableName:data.TableName,
		Rows:data.Rows,
		BinlogFileNum:BinlogFileNum,
		BinlogPosition:data.Header.LogPos,
		Query:data.Query,
		Gtid:data.Gtid,
		Pri:data.Pri,
		ColumnMapping: data.ColumnMapping,
		EventID: data.EventID,
	}
	c.callback(data0)
}