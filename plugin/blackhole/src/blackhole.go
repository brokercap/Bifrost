package src

import (
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
)

const VERSION = "v1.6.0"
const BIFROST_VERION = "v1.6.0"

func init() {
	pluginDriver.Register("blackhole", NewConn, VERSION, BIFROST_VERION)
}

func NewConn() pluginDriver.Driver {
	return &Conn{}
}

type Conn struct {
	pluginDriver.PluginDriverInterface
}

func (c *Conn) CommitBatch(dataList []*pluginDriver.PluginDataType, retry bool) (LastSuccessCommitData *pluginDriver.PluginDataType, ErrData *pluginDriver.PluginDataType, err error) {
	var tmpLastSuccessCommitData *pluginDriver.PluginDataType
	for i := range dataList {
		data := dataList[i]
		switch data.EventType {
		case "insert":
			LastSuccessCommitData, ErrData, err = c.Insert(data, retry)

		case "update":
			LastSuccessCommitData, ErrData, err = c.Update(data, retry)
		case "delete":
			LastSuccessCommitData, ErrData, err = c.Del(data, retry)
		case "sql":
			LastSuccessCommitData, ErrData, err = c.Query(data, retry)
		case "commit":
			LastSuccessCommitData, ErrData, err = c.Commit(data, retry)
		default:
			continue
		}
		if err != nil {
			return
		}
		// 因为是正序排的,所以前面的位点,是被处理过的,这里将当前执行成功的位点,保存为最后一个成功执行的位点
		// 即使后面的循环中出错,也可以将最后一个成功的位点返回回去,
		if tmpLastSuccessCommitData != nil {
			LastSuccessCommitData = tmpLastSuccessCommitData
		}
	}
	return
}
