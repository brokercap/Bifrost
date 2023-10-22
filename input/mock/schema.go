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

import (
	"fmt"
	"github.com/brokercap/Bifrost/sdk/pluginTestData"
	"time"

	inputDriver "github.com/brokercap/Bifrost/input/driver"
)

func (c *InputMock) GetNormalTableObjlist() (tableList []*NormalTable) {
	normalTable := &NormalTable{
		SchemaName:    DefaultNormalSchemaName,
		TableName:     "normal",
		LongStringLen: c.config.LongStringLen,
	}
	normalTableNoMapping := &NormalTable{
		SchemaName:    DefaultNormalSchemaName,
		TableName:     "no_mapping",
		LongStringLen: c.config.LongStringLen,
	}
	normalTableNoPks := &NormalTable{
		SchemaName:    DefaultNormalSchemaName,
		TableName:     "no_pks",
		NoPks:         true,
		LongStringLen: c.config.LongStringLen,
	}
	tableList = append(tableList, normalTable)
	tableList = append(tableList, normalTableNoMapping)
	tableList = append(tableList, normalTableNoPks)
	return
}

func (c *InputMock) GetNormalTableList() (tableList []inputDriver.TableList) {
	for _, t := range c.GetNormalTableObjlist() {
		tableList = append(tableList,
			inputDriver.TableList{
				TableName: t.TableName,
			})
	}
	return
}

func (c *InputMock) GetPerformanceDatabasenNameList() (data []string) {
	for i := 0; i < c.config.GetPerformanceDatabaseCount(); i++ {
		data = append(data, fmt.Sprintf("%s_%d", c.config.GetPerformanceDatabasePrefix(), i+1))
	}
	return
}

func (c *InputMock) GetPerformanceTableList() (tableList []inputDriver.TableList) {
	for i := 0; i < c.config.GetPerformanceDatabaseTableCount(); i++ {
		tableList = append(tableList,
			inputDriver.TableList{
				TableName: fmt.Sprintf("%s_%d", c.config.GetPerformanceTablePrefix(), i+1),
			})
	}
	return
}

func (c *InputMock) GetSchemaList() (data []string, err error) {
	data = append(data, DefaultNormalSchemaName)
	data = append(data, c.GetPerformanceDatabasenNameList()...)
	return
}

func (c *InputMock) GetSchemaTableList(schema string) (tableList []inputDriver.TableList, err error) {
	if schema == DefaultNormalSchemaName {
		return c.GetNormalTableList(), nil
	}
	return c.GetPerformanceTableList(), nil
}

func (c *InputMock) GetSchemaTableFieldList(schema string, table string) (FieldList []inputDriver.TableFieldInfo, err error) {
	tableList, _ := c.GetSchemaTableList(schema)
	var ok bool
	for _, v := range tableList {
		if v.TableName == table {
			ok = true
			break
		}
	}
	// 如果table 不在所有table范围内,有可能传进来的table 是 .* 这样的别名,这个时候应该返回空
	if !ok {
		FieldList = make([]inputDriver.TableFieldInfo, 0)
		return
	}
	event := pluginTestData.NewEvent()
	event.SetSchema(schema)
	event.SetTable(table)
	for _, ColumnInfo := range event.ColumnList {
		var ColumnDefault *string
		if ColumnInfo.ColumnDefault != "NULL" {
			ColumnDefault = &ColumnInfo.ColumnDefault
		}
		var IsNullable bool
		switch ColumnInfo.IsNullable {
		case "true", "TRUE":
			IsNullable = true
		default:
			break
		}
		var NumericPrecision = uint64(ColumnInfo.NumbericPrecision)
		var NumericScale = uint64(ColumnInfo.NumbericScale)

		TableFieldInfo := inputDriver.TableFieldInfo{
			ColumnName:       &ColumnInfo.ColumnName,
			ColumnDefault:    ColumnDefault,
			IsNullable:       IsNullable,
			ColumnType:       &ColumnInfo.ColumnType,
			IsAutoIncrement:  ColumnInfo.AutoIncrement,
			DataType:         &ColumnInfo.DataType,
			NumericPrecision: &NumericPrecision,
			NumericScale:     &NumericScale,
			ColumnKey:        &ColumnInfo.ColumnKey,
		}
		FieldList = append(FieldList, TableFieldInfo)
	}
	return
}

func (c *InputMock) CheckPrivilege() (err error) {
	return
}

func (c *InputMock) CheckUri(CheckPrivilege bool) (CheckUriResult inputDriver.CheckUriResult, err error) {
	result := inputDriver.CheckUriResult{
		BinlogFile:     DefaultBinlogFileName,
		BinlogPosition: 0,
		Gtid:           fmt.Sprint(time.Now().Unix()),
		ServerId:       1,
		BinlogFormat:   "row",
		BinlogRowImage: "full",
	}
	return result, nil
}

func (c *InputMock) GetVersion() (Version string, err error) {
	return "mysql-8.0", nil
}
