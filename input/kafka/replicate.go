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

import "fmt"

func (c *InputKafka) GetTopics() (topics []string, err error) {
	topics, err = c.GetTopics0()
	if err != nil {
		return
	}
	if len(topics) > 0 {
		return
	}
	return c.GetTopics1()
}

func (c *InputKafka) GetTopics0() (topics []string, err error) {
	c.Lock()
	defer c.Unlock()
	if _, ok := c.topics["*"]; !ok {
		return
	}
	topics, err = c.GetSchemaList()
	if err != nil {
		return
	}
	if len(topics) == 0 {
		err = fmt.Errorf("not found topics")
	}
	return
}

func (c *InputKafka) GetTopics1() (topics []string, err error) {
	c.Lock()
	defer c.Unlock()
	for topic, _ := range c.topics {
		topics = append(topics, topic)
	}
	return
}

func (c *InputKafka) AddReplicateDoDb(SchemaName, TableName string) (err error) {
	c.Lock()
	defer c.Unlock()
	if _, ok := c.topics[SchemaName]; !ok {
		c.topics[SchemaName] = make(map[string]bool, 0)
	}
	c.topics[SchemaName][TableName] = true
	return nil
}

func (c *InputKafka) DelReplicateDoDb(SchemaName, TableName string) (err error) {
	defer c.Unlock()
	if _, ok := c.topics[SchemaName]; !ok {
		return
	}
	delete(c.topics[SchemaName], TableName)
	return nil
}
