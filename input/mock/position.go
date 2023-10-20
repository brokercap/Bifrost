package mock

import (
	inputDriver "github.com/brokercap/Bifrost/input/driver"
)

func (c *InputMock) GetLastPosition() *inputDriver.PluginPosition {
	return c.lastSuccessPosition
}

func (c *InputMock) DoneMinPosition(p *inputDriver.PluginPosition) (err error) {
	if p == nil {
		return
	}
	p.BinlogFileName = DefaultBinlogFileName
	c.lastSuccessPosition = p
	return nil
}

// 获取队列最新的位点

func (c *InputMock) GetCurrentPosition() (p *inputDriver.PluginPosition, err error) {
	p = &inputDriver.PluginPosition{
		GTID:           "",
		BinlogFileName: DefaultBinlogFileName,
		BinlogPostion:  c.lastEventEndTime,
		EventID:        c.eventID,
	}
	return
}
