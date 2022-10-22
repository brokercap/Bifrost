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
	"strings"
)

func (c *InputKafka) SetTopicPartitionOffsetAndReturnGTID(kafkaMsg *sarama.ConsumerMessage) (GTID string) {
	c.Lock()
	defer c.Unlock()
	if kafkaMsg != nil {
		var ok bool
		if _, ok = c.positionMap[kafkaMsg.Topic]; !ok {
			c.positionMap[kafkaMsg.Topic] = make(map[int32]int64, 0)
		}
		c.positionMap[kafkaMsg.Topic][kafkaMsg.Partition] = kafkaMsg.Offset
	}
	return c.positionMapToGTID(c.positionMap)
}

func (c *InputKafka) positionMapToGTID(positionMap map[string]map[int32]int64) (GTID string) {
	var gtids = make([]string, 0)
	for topic, p := range positionMap {
		for partition, offset := range p {
			gtids = append(gtids, fmt.Sprintf("%s:%d:%d", topic, partition, offset))
		}
	}
	return strings.Join(gtids, ",")
}
