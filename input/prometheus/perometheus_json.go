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

package prometheus

import (
	"encoding/json"

	outputDriver "github.com/brokercap/Bifrost/plugin/driver"
)

type PrometheusObject struct {
	Status string         `json:"status"`
	Data   PrometheusData `json:"data"`
	size   uint32
}

type PrometheusData struct {
	ResultType string `json:"resultType"`
	Result     []PrometheusDataResult
}

type PrometheusDataResult struct {
	Metric map[string]string `json:"metric"`
	Values []*[2]interface{} `json:"values"`
	Value  *[2]interface{}   `json:"value"`
}

func NewPrometheusObject(body []byte) *PrometheusObject {
	var data PrometheusObject
	json.Unmarshal(body, &data)
	data.size = uint32(len(body))
	return &data
}

func (c *PrometheusObject) ReadAndToBifrostData() chan *outputDriver.PluginDataType {
	ch := make(chan *outputDriver.PluginDataType, 1000)
	go c.ReadAndToBifrostData0(ch)
	return ch
}

func (c *PrometheusObject) ReadAndToBifrostData0(ch chan *outputDriver.PluginDataType) {
	defer func() {
		close(ch)
	}()
	var size uint32 = 0
	for i, resultInfo := range c.Data.Result {
		if i == 0 {
			size = c.size
		} else {
			size = 0
		}
		var keyCount = len(resultInfo.Metric) + 2
		var columnMapping = make(map[string]string, keyCount)
		var pks = make([]string, 0, keyCount)
		if resultInfo.Value != nil {
			// 假如只有Value的情况下，说明只有一条记录，这里为了Coding方便，转成 Values 统一计算
			resultInfo.Values = []*[2]interface{}{resultInfo.Value}
		}
		for _, value := range resultInfo.Values {
			var row = make(map[string]interface{}, keyCount)
			// [时间戳，值] json反解析的时候在没指定类型的情况下，int会被转成float64类型，所以这里要转进行转换一次
			timestamp := uint32(value[0].(float64))
			if len(columnMapping) == 0 {
				for key, val := range resultInfo.Metric {
					row[key] = val
					columnMapping[key] = "string"
					pks = append(pks, key)
				}
				pks = append(pks, "_tsdb_timestamp")
				row["_tsdb_timestamp"] = timestamp
				row["_tsdb_value"] = value[1]
				columnMapping["_tsdb_timestamp"] = "int"
				columnMapping["_tsdb_value"] = "string"
			} else {
				for key, val := range resultInfo.Metric {
					row[key] = val
				}
				row["_tsdb_timestamp"] = timestamp
				row["_tsdb_value"] = value[1]
			}
			tableName := resultInfo.Metric["__name__"]
			data := &outputDriver.PluginDataType{
				Timestamp:       timestamp,
				EventSize:       size,
				EventType:       EventType,
				Rows:            []map[string]interface{}{row},
				SchemaName:      DefaultSchemaName,
				TableName:       tableName,
				AliasSchemaName: DefaultSchemaName,
				AliasTableName:  tableName,
				BinlogFileNum:   1,
				BinlogPosition:  timestamp,
				Pri:             pks,
				ColumnMapping:   columnMapping,
			}
			ch <- data
		}
	}
}
