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
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"

	inputDriver "github.com/brokercap/Bifrost/input/driver"
)

func (c *InputKafka) GetConn() (sarama.Client, error) {
	if c.config == nil {
		return nil, fmt.Errorf("kafka config init err")
	}
	if len(c.config.BrokerServerList) == 0 {
		return nil, fmt.Errorf("kafka broker server is empty")
	}
	client, err := sarama.NewClient(c.config.BrokerServerList, c.config.ParamConfig)
	return client, err
}

func (c *InputKafka) GetSchemaList() (data []string, err error) {
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

func (c *InputKafka) GetSchemaTableList(schema string) (tableList []inputDriver.TableList, err error) {
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
			TableName: c.FormatPartitionTableName(partition),
			TableType: "",
		})
	}
	return tableList, nil
}

func (c *InputKafka) GetSchemaTableFieldList(schema string, table string) (FieldList []inputDriver.TableFieldInfo, err error) {
	return make([]inputDriver.TableFieldInfo, 0), nil
}

func (c *InputKafka) CheckPrivilege() (err error) {
	return
}

func (c *InputKafka) CheckUri(CheckPrivilege bool) (CheckUriResult inputDriver.CheckUriResult, err error) {
	if c.err != nil {
		err = c.err
		return
	}
	client, err := c.GetConn()
	if err != nil {
		return CheckUriResult, err
	}
	defer client.Close()
	result := inputDriver.CheckUriResult{
		BinlogFile:     DefaultBinlogFileName,
		BinlogPosition: DefaultBinlogPosition,
		Gtid:           "",
		ServerId:       1,
		BinlogFormat:   "row",
		BinlogRowImage: "full",
	}
	return result, nil
}

// 获取队列最新的位点

func (c *InputKafka) GetCurrentPosition() (p *inputDriver.PluginPosition, err error) {
	var topics []string
	topics, err = c.GetTopics()
	if err != nil {
		return nil, err
	}
	if len(topics) <= 0 {
		return nil, fmt.Errorf("not found topics")
	}
	positionMap := make(map[string]map[int32]int64, 0)
	client, err0 := c.GetConn()
	if err0 != nil {
		return nil, err0
	}
	defer client.Close()
	for _, topicName := range topics {
		partitionArr, err := client.Partitions(topicName)
		if err != nil {
			continue
		}
		var ok bool
		var partitionOffsetMap map[int32]int64
		if partitionOffsetMap, ok = positionMap[topicName]; !ok {
			partitionOffsetMap = make(map[int32]int64, 0)
			positionMap[topicName] = partitionOffsetMap
		}
		for _, partition := range partitionArr {
			offset, err := client.GetOffset(topicName, partition, sarama.OffsetNewest)
			if err != nil {
				continue
			}
			positionMap[topicName][partition] = offset
		}
	}
	gtids := c.positionMapToGTID(positionMap)
	p = &inputDriver.PluginPosition{
		GTID:           gtids,
		BinlogFileName: DefaultBinlogFileName,
		BinlogPostion:  DefaultBinlogPosition,
		Timestamp:      uint32(time.Now().Unix()),
		EventID:        c.eventID,
	}
	return p, nil
}

func (c *InputKafka) GetVersion() (Version string, err error) {
	client, err := c.GetConn()
	if err != nil {
		return Version, err
	}
	defer client.Close()
	Version = client.Config().Version.String()
	return
}

func (c *InputKafka) FormatPartitionTableName(partition int32) string {
	return fmt.Sprintf(partitionTableNamePrefix+"%d", partition)
}

func (c *InputKafka) GetPartitionByTableName(partitionTableName string) (partition int32) {
	partitionStr := strings.ReplaceAll(partitionTableName, partitionTableNamePrefix, "")
	tmpInt, _ := strconv.Atoi(partitionStr)
	return int32(tmpInt)
}
