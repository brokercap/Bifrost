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
	"fmt"
	inputDriver "github.com/brokercap/Bifrost/input/driver"
	"sync/atomic"
)

func (c *InputMock) setStatus(status inputDriver.StatusFlag) {
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

func (c *InputMock) Stop() error {
	c.setStatus(inputDriver.STOPPED)
	return nil
}

func (c *InputMock) Close() error {
	if c.inputCancelFun != nil {
		c.inputCancelFun()
		c.inputCancelFun = nil
	}
	c.tableMap = nil
	c.setStatus(inputDriver.CLOSED)
	return nil
}

func (c *InputMock) Kill() error {
	return c.Close()
}

func (c *InputMock) SetEventID(eventId uint64) error {
	c.eventID = eventId
	return nil
}

func (c *InputMock) getNextEventID() uint64 {
	atomic.AddUint64(&c.eventID, 1)
	return c.eventID
}
