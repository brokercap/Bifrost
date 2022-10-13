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
package kafka

import (
	"fmt"

	"github.com/Shopify/sarama"

	inputDriver "github.com/brokercap/Bifrost/input/driver"
)

func (c *Input) GetConn() (sarama.Client, error) {
	if c.config == nil {
		return nil, fmt.Errorf("kafka config init err")
	}
	if len(c.config.BrokerServerList) == 0 {
		return nil, fmt.Errorf("kafka broker server is empty")
	}
	client, err := sarama.NewClient(c.config.BrokerServerList, c.config.ParamConfig)
	return client, err
}

func (c *Input) GetSchemaList() (data []string, err error) {
	// 假如连接的时候有指定Topics列表，则指定Topics
	if c.config != nil && len(c.config.Topics) > 0 {
		return c.config.Topics, nil
	}
	client, err := c.GetConn()
	if err != nil {
		return data, err
	}
	defer client.Close()
	return client.Topics()
}

func (c *Input) GetSchemaTableList(schema string) (tableList []inputDriver.TableList, err error) {
	client, err := c.GetConn()
	if err != nil {
		return tableList, err
	}
	defer client.Close()
	partitionsArr, err := client.Partitions(schema)
	if err != nil {
		return tableList, err
	}
	for _, partition := range partitionsArr {
		tableList = append(tableList, inputDriver.TableList{
			TableName: fmt.Sprintf("partition_%d", partition),
			TableType: "",
		})
	}
	return tableList, nil
}

func (c *Input) GetSchemaTableFieldList(schema string, table string) (FieldList []inputDriver.TableFieldInfo, err error) {
	return make([]inputDriver.TableFieldInfo, 0), nil
}

func (c *Input) CheckPrivilege() (err error) {
	return
}

func (c *Input) CheckUri(CheckPrivilege bool) (CheckUriResult inputDriver.CheckUriResult, err error) {
	client, err := c.GetConn()
	if err != nil {
		return CheckUriResult, err
	}
	defer client.Close()
	result := inputDriver.CheckUriResult{
		BinlogFile:     "bifrost.000001",
		BinlogPosition: 0,
		Gtid:           "",
		ServerId:       1,
		BinlogFormat:   "row",
		BinlogRowImage: "full",
	}
	return result, nil
}

func (c *Input) GetCurrentPosition() (p *inputDriver.PluginPosition, err error) {
	return
}

func (c *Input) GetVersion() (Version string, err error) {
	client, err := c.GetConn()
	if err != nil {
		return Version, err
	}
	defer client.Close()
	Version = client.Config().Version.String()
	return
}

func (c *Input) FormatPartitionTableName(partition int32) string {
	return fmt.Sprintf(partitionTableNamePrefix+"%d", partition)
}
