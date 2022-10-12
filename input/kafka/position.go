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
	"strconv"
	"strings"
)

func (c *Input) SetTopicPartitionOffsetAndReturnGTID(kafkaMsg *sarama.ConsumerMessage) (GTID string) {
	c.Lock()
	defer c.Unlock()
	var ok bool
	if _, ok = c.positionMap[kafkaMsg.Topic]; !ok {
		c.positionMap[kafkaMsg.Topic] = make(map[int32]int64, 0)
	}
	c.positionMap[kafkaMsg.Topic][kafkaMsg.Partition] = kafkaMsg.Offset

	var gtids = make([]string, 0)
	for topic, p := range c.positionMap {
		for partition, offset := range p {
			gtids = append(gtids, fmt.Sprintf("%s:%d:%d", topic, partition, offset))
		}
	}
	return strings.Join(gtids, ",")
}

func (c *Input) DoneMinPosition(p *inputDriver.PluginPosition) (err error) {
	if p.GTID == "" {
		return
	}
	for _, gtid := range strings.Split(p.GTID, ",") {
		gtidInfoArr := strings.Split(gtid, ":")
		if len(gtidInfoArr) != 3 {
			continue
		}
		partition, err := strconv.ParseInt(gtidInfoArr[1], 10, 32)
		if err != nil {
			continue
		}
		offset, err := strconv.ParseInt(gtidInfoArr[2], 10, 64)
		if err != nil {
			continue
		}
		c.waitCommitOffset <- &waitCommitOffset{
			topic:     gtidInfoArr[0],
			partition: int32(partition),
			offset:    offset,
		}
	}
	return nil
}
