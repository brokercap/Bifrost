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

import "encoding/json"

type Config struct {
	PerformanceDatabaseCount               int    `json:"PerformanceDatabaseCount,string"`
	PerformanceDatabaseTableCount          int    `json:"PerformanceDatabaseTableCount,string"`
	PerformanceDatabasePrefix              string `json:"PerformanceDatabasePrefix,string"`
	PerformanceTablePrefix                 string `json:"PerformanceTablePrefix,string"`
	PerformanceTableDataCount              int    `json:"PerformanceTableDataCount,string"`
	PerformanceTableRowsEventCount         int    `json:"PerformanceTableRowsEventCount,string"`
	PerformanceTableDeleteEventRatio       int    `json:"PerformanceTableDeleteEventRatio,string"`
	PerformanceTableRowsEventBatchSize     int    `json:"PerformanceTableRowsEventBatchSize,string"`
	PerformanceTableRowsEventBatchInterval int    `json:"PerformanceTableRowsEventBatchInterval,string"`
	LongStringLen                          int    `json:"LongStringLen,string"`
	IsAllInsertSameData                    bool   `json:"IsAllInsertSameData,string"`
}

func NewConfig(configMap map[string]string) *Config {
	c, _ := json.Marshal(configMap)
	var config Config
	json.Unmarshal(c, &config)
	config.InitDefault()
	return &config
}

func (c *Config) InitDefault() {
	if c.PerformanceDatabaseCount <= 0 {
		c.PerformanceDatabaseCount = 1
	}
	if c.PerformanceDatabaseTableCount <= 0 {
		c.PerformanceDatabaseTableCount = 1
	}
	if c.PerformanceDatabasePrefix == "" {
		c.PerformanceDatabasePrefix = "performance"
	}
	if c.PerformanceTablePrefix == "" {
		c.PerformanceTablePrefix = "tb"
	}
	if c.PerformanceTableDataCount <= 0 {
		c.PerformanceTableDataCount = 1000000
	}
	if c.PerformanceTableRowsEventCount <= 0 {
		c.PerformanceTableRowsEventCount = c.PerformanceTableDataCount
	}
	if c.PerformanceTableRowsEventBatchSize <= 0 {
		c.PerformanceTableRowsEventBatchSize = 1000
	}
	if c.PerformanceTableRowsEventBatchInterval <= 0 {
		c.PerformanceTableRowsEventBatchInterval = 60
	}
}

func (c *Config) GetPerformanceDatabaseCount() int {
	return c.PerformanceDatabaseCount
}

func (c *Config) GetPerformanceDatabaseTableCount() int {
	return c.PerformanceDatabaseTableCount
}

func (c *Config) GetPerformanceDatabasePrefix() string {
	return c.PerformanceDatabasePrefix
}

func (c *Config) GetPerformanceTablePrefix() string {
	return c.PerformanceTablePrefix
}

func (c *Config) GetPerformanceTableDataCount() int {
	return c.PerformanceTableDataCount
}

func (c *Config) GetPerformanceTableRowsEventCount() int {
	return c.PerformanceTableRowsEventCount
}

func (c *Config) GetPerformanceTableDeleteEventRatio() int {
	return c.PerformanceTableDeleteEventRatio
}

func (c *Config) GetPerformanceTableRowsEventBatchSize() int {
	return c.PerformanceTableRowsEventBatchSize
}

func (c *Config) GetPerformanceTableRowsEventBatchInterval() int {
	return c.PerformanceTableRowsEventBatchInterval
}
