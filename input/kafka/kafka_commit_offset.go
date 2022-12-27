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
	"context"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"

	inputDriver "github.com/brokercap/Bifrost/input/driver"
)

func (c *InputKafka) ConsumePluginPosition(sess sarama.ConsumerGroupSession, ctx context.Context) {
	log.Printf("DbName:%s ConsumePluginPosition starting \r\n", c.inputInfo.DbName)
	defer func() {
		log.Printf("DbName:%s ConsumePluginPosition over \r\n", c.inputInfo.DbName)
	}()
	var lastEventId uint64 = 0
	for {
		select {
		case p := <-c.waitCommitOffset:
			if p == nil {
				return
			}
			// 由上一层定时将最小的位点提交回input 插件层，所以一直没有数据，一直重新重复提交相同的位点进来
			// 所以这里需要判断一下只要和上一次eventId不一样，则需要保存
			// 这里为什么不判断 > lastEventId ，是因为存在可能EventID被更新了，强制变小了的可能
			if p.EventID == lastEventId {
				break
			}
			data := c.TransferWaitCommitOffsetList(p)
			for _, pluginPosition := range data {
				sess.MarkOffset(pluginPosition.topic, pluginPosition.partition, pluginPosition.offset+1, "")
				sess.Commit()
			}
			lastEventId = p.EventID
			break
		case <-ctx.Done():
			return
		}
	}
}

func (c *InputKafka) TransferWaitCommitOffsetList(p *inputDriver.PluginPosition) (data []*waitCommitOffset) {
	if p == nil {
		return
	}
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
		data = append(data, &waitCommitOffset{
			topic:     gtidInfoArr[0],
			partition: int32(partition),
			offset:    offset,
		})
	}
	return
}

func (c *InputKafka) DoneMinPosition(p *inputDriver.PluginPosition) (err error) {
	if p == nil {
		return
	}
	p.BinlogFileName = "bifrost.000001"
	c.lastOffset = p
	// 这里加一个超时，防止 waitCommitOffset 被阻塞，导致上一层被阻塞
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	select {
	case c.waitCommitOffset <- p:
		break
	case <-timer.C:
		break
	}
	return nil
}
