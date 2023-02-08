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
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	inputDriver "github.com/brokercap/Bifrost/input/driver"
	outputDriver "github.com/brokercap/Bifrost/plugin/driver"
)

type InputPrometheus struct {
	sync.RWMutex
	inputDriver.PluginDriverInterface
	inputInfo        inputDriver.InputInfo
	status           inputDriver.StatusFlag
	err              error
	PluginStatusChan chan *inputDriver.PluginStatus
	eventID          uint64

	config *Config

	callback inputDriver.Callback

	inputCtx       context.Context
	inputCancelFun context.CancelFunc

	positionMap map[string]uint32

	lastEventData *outputDriver.PluginDataType
}

func NewInputPrometheus() inputDriver.Driver {
	c := &InputPrometheus{}
	c.Init()
	return c
}

func (c *InputPrometheus) GetUriExample() (string, string) {
	notesHtml := `<p>input.time.interval : 间隔时间获取数据时间，单位/秒,默认300s</p>
<p>BinlogPosition : 请填写开始时间戳，精确到秒,默认为 Now() - input.time.interval </p>
`
	return "http://127.0.0.1:9090/api/v1/query?query=target&input.time.interval=300", notesHtml
}

func (c *InputPrometheus) Init() {
	c.positionMap = make(map[string]uint32, 0)
}

func (c *InputPrometheus) SetOption(inputInfo inputDriver.InputInfo, param map[string]interface{}) {
	dsnMap := ParseDSN(inputInfo.ConnectUri)
	c.config, c.err = getConfig(dsnMap)
	c.inputInfo = inputInfo
	c.config.lastSuccessEndTime = int(inputInfo.BinlogPostion)
}

func (c *InputPrometheus) setStatus(status inputDriver.StatusFlag) {
	c.status = status
	switch status {
	case inputDriver.CLOSED:
		c.err = fmt.Errorf("")
		break
	}
	if c.PluginStatusChan != nil {
		c.PluginStatusChan <- &inputDriver.PluginStatus{Status: status, Error: c.err}
	}
}

func (c *InputPrometheus) Start(ch chan *inputDriver.PluginStatus) error {
	c.PluginStatusChan = ch
	return c.Start0()
}

func (c *InputPrometheus) Start0() error {
	c.inputCtx, c.inputCancelFun = context.WithCancel(context.Background())
	var timer *time.Timer
	timeInterval := time.Duration(c.config.TimeInterval) * time.Second
	timer = time.NewTimer(timeInterval)
	defer timer.Stop()
	defer func() {
		c.setStatus(inputDriver.STOPPED)
		c.setStatus(inputDriver.CLOSED)
	}()
	c.setStatus(inputDriver.STARTING)
	c.setStatus(inputDriver.RUNNING)
	for {
		c.Start1()
		if c.config.End > 0 {
			break
		}
		timer.Reset(timeInterval)
		select {
		case <-c.inputCtx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
			continue
		}
	}
	return nil
}

func (c *InputPrometheus) Start1() error {
	body, _, err := c.GetPrometheusBody()
	if err != nil {
		c.err = err
		return err
	}
	c.Callback(body)
	return nil
}

func (c *InputPrometheus) GetStartTime() int {
	if c.config.Start > 0 && c.config.lastSuccessEndTime == 0 {
		return c.config.Start
	}
	if c.config.lastSuccessEndTime == 0 {
		c.config.lastSuccessEndTime = int(time.Now().Unix()) - c.config.TimeInterval - 60
	}
	return c.config.lastSuccessEndTime
}

func (c *InputPrometheus) GetEndTime() int {
	if c.config.End > 0 {
		return c.config.End
	}
	c.config.lastTmpEndTime = int(time.Now().Unix() - 60)
	return c.config.lastTmpEndTime
}

func (c *InputPrometheus) GetUrl() string {
	return fmt.Sprintf("%s&start=%d&end=%d", c.config.Url, c.GetStartTime(), c.GetEndTime())
}

func (c *InputPrometheus) GetPrometheusBody() (body []byte, httpCode int, err error) {
	url := c.GetUrl()
	client := &http.Client{Timeout: c.config.HttpTimeout}
	var req *http.Request
	req, err = http.NewRequest("GET", url, nil)
	if err != nil {
		return
	}
	var resp *http.Response
	resp, err = client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	body, err = io.ReadAll(resp.Body)
	httpCode = resp.StatusCode
	return
}

func (c *InputPrometheus) Stop() error {
	c.setStatus(inputDriver.STOPPING)
	if c.inputCancelFun != nil {
		c.inputCancelFun()
		c.inputCancelFun = nil
	}
	return nil
}

func (c *InputPrometheus) Close() error {
	c.Stop()
	c.setStatus(inputDriver.CLOSED)
	return nil
}

func (c *InputPrometheus) Kill() error {
	return c.Close()
}

func (c *InputPrometheus) GetLastPosition() *inputDriver.PluginPosition {
	if c.config.lastSuccessEndTime == 0 {
		return nil
	}
	return &inputDriver.PluginPosition{
		GTID:           "",
		BinlogFileName: DefaultBinlogFileName,
		BinlogPostion:  uint32(c.config.lastSuccessEndTime),
		Timestamp:      uint32(time.Now().Unix()),
		EventID:        c.eventID,
	}
}

func (c *InputPrometheus) SetEventID(eventId uint64) error {
	c.eventID = eventId
	return nil
}

func (c *InputPrometheus) getNextEventID() uint64 {
	atomic.AddUint64(&c.eventID, 1)
	return c.eventID
}
