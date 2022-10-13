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

import "github.com/Shopify/sarama"

func (c *Input) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Input) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Input) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case kafkaMsg := <-claim.Messages():
			c.InputCallback(kafkaMsg)
			break
		case p := <-c.waitCommitOffset:
			sess.MarkOffset(p.topic, p.partition, p.offset, "")
			break
		}
	}
	return nil
}