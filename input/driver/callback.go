package driver

import (
	outputDriver "github.com/brokercap/Bifrost/plugin/driver"
)

type Callback func(data *outputDriver.PluginDataType)
