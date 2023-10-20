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
	"context"
	"errors"
	"fmt"
	inputDriver "github.com/brokercap/Bifrost/input/driver"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"sync"
	"time"
)

type InputMock struct {
	sync.RWMutex
	inputDriver.PluginDriverInterface
	inputInfo inputDriver.InputInfo

	callback inputDriver.Callback

	status           inputDriver.StatusFlag
	err              error
	PluginStatusChan chan *inputDriver.PluginStatus
	eventID          uint64
	ws               sync.WaitGroup
	wsHadInit        bool
	inputCtx         context.Context
	inputCancelFun   context.CancelFunc
	tableDataChan    chan *pluginDriver.PluginDataType

	config *Config

	tableMap map[string]Table

	lastEventEndTime    uint32
	lastSuccessPosition *inputDriver.PluginPosition
}

func NewInputMock() inputDriver.Driver {
	c := &InputMock{
		tableDataChan: make(chan *pluginDriver.PluginDataType, 10000),
		tableMap:      make(map[string]Table, 0),
	}
	c.inputCtx, c.inputCancelFun = context.WithCancel(context.Background())
	go c.ConsumeTableData()
	return c
}

func (c *InputMock) GetUriExample() (example string, notesHtml string) {
	example = "PerformanceTableDataCount=1000000&PerformanceTableRowsEventBatchInterval=60&PerformanceTableRowsEventBatchSize=1000"
	notesHtml = `<p>PerformanceDatabaseCount: 性能测试库的数量,默认 1</p>
				<p>PerformanceDatabaseTableCount: 每个性能测试库的表数量,默认 1</p>
				<p>PerformanceTableDataCount: 每个性能测试表的数据条数,默认 100万</p>
				<p>PerformanceTableRowsEventBatchInterval: 每个性能测试表 间隔多少秒产生一批数据,单位为秒,默认 60秒</p>
				<p>PerformanceTableRowsEventBatchSize: 每个性能测试表 每个批次产生多少条数据,默认 1000</p>
				<p>PerformanceTableRowsEventCount: 每个性能测试表 最大产生多少个 RowsEvent 事件,默认 和 PerformanceTableDataCount 一致</p>
				<p>PerformanceTableDeleteEventRatio: 每个性能测试表 删除事件产生的概率,PerformanceTableRowsEventCount > PerformanceTableDataCount 的时候生效,默认 0</p>
				<p>LongStringLen: 长字符串的字符个数,大于0的时候,则为开启长字符串生成,默认为0,</p>
				<p>IsAllInsertSameData: 每个批次只生成相同的Insert事件数据,自增ID等均不变,主要用于测试无视数据变更的测试场景</p>
				<p>&nbsp;</p>
				<p>normal: 有一个主键,包括绝大部分类型的字段</p>
				<p>no_mapping: 数据中,没有 ColumnMapping</p>
				<p>no_pks: 没有主键</p>
	`
	return
}

func (c *InputMock) SetOption(inputInfo inputDriver.InputInfo, param map[string]interface{}) {
	configMap := ParseDSN(inputInfo.ConnectUri)
	c.config = NewConfig(configMap)
	c.inputInfo = inputInfo
}

func (c *InputMock) Start(ch chan *inputDriver.PluginStatus) error {
	c.PluginStatusChan = ch
	return c.Start0()
}

func (c *InputMock) Start0() error {
	c.setStatus(inputDriver.STARTING)
	c.setStatus(inputDriver.RUNNING)
	c.StartNormalTables()
	c.StartPerformanceTables()
	if len(c.tableMap) == 0 {
		c.Close()
		return errors.New("no table setting sync")
	}
	go c.TableTaskWait()
	return nil
}

func (c *InputMock) TableTaskWait() {
	c.Lock()
	if c.wsHadInit {
		c.Unlock()
		return
	}
	c.wsHadInit = true
	c.Unlock()
	defer func() {
		c.Lock()
		c.wsHadInit = false
		c.Unlock()
	}()
	c.ws.Wait()
	c.Stop()
}

func (c *InputMock) StartNormalTables() {
	tableList := c.GetNormalTableObjlist()
	for _, t := range tableList {
		if !c.CheckReplicateDb(t.SchemaName, t.TableName) {
			continue
		}
		c.StartTable(t)
	}
}

func (c *InputMock) StartPerformanceTables() {
	for _, schema := range c.GetPerformanceDatabasenNameList() {
		tableList, _ := c.GetSchemaTableList(schema)
		for _, t := range tableList {
			if !c.CheckReplicateDb(schema, t.TableName) {
				continue
			}
			tObj := c.NewPerformanceTableObj(schema, t.TableName)
			c.StartTable(tObj)
		}
	}
}

func (c *InputMock) NewPerformanceTableObj(schemaName, tableName string) *PerformanceTable {
	return &PerformanceTable{
		SchemaName:          schemaName,
		TableName:           tableName,
		LongStringLen:       c.config.LongStringLen,
		IsAllInsertSameData: c.config.IsAllInsertSameData,
		TableDataCount:      c.config.GetPerformanceTableDataCount(),
		TableRowsEventCount: c.config.GetPerformanceTableRowsEventCount(),
		DeleteEventRatio:    c.config.GetPerformanceTableDeleteEventRatio(),
		BatchSize:           c.config.GetPerformanceTableRowsEventBatchSize(),
		InterVal:            time.Duration(c.config.GetPerformanceTableRowsEventBatchInterval()) * time.Second,
	}
}

func (c *InputMock) StartTable(t Table) {
	c.Lock()
	defer c.Unlock()
	key := fmt.Sprintf("%s_%s", t.GetSchemaName(), t.GetTableName())
	if _, ok := c.tableMap[key]; ok {
		return
	}
	c.tableMap[key] = t
	c.ws.Add(1)
	go func() {
		defer c.ws.Done()
		t.Start(c.inputCtx, c.tableDataChan)
	}()
}

func (c *InputMock) StartTableWithName(schemaName, tableName string) {
	if schemaName == DefaultNormalSchemaName {
		c.StartNormalTables()
		return
	}
	c.StartPerformanceTables()
}
