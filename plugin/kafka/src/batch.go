package src

import "github.com/brokercap/Bifrost/plugin/driver"

func (c *Conn) CommitBatch(dataList []*driver.PluginDataType, retry bool) (LastSuccessCommitData *driver.PluginDataType, ErrData *driver.PluginDataType, err error) {
	var tmpLastSuccessCommitData *driver.PluginDataType
	c.p.BatchSize = len(dataList)
	for i := range dataList {
		data := dataList[i]
		var isCommit = false
		switch data.EventType {
		case "commit":
			isCommit = true
		default:
			break
		}
		tmpLastSuccessCommitData, ErrData, err = c.SendToList(dataList[i], retry, isCommit)
		if tmpLastSuccessCommitData != nil {
			LastSuccessCommitData = tmpLastSuccessCommitData
		}
		if err != nil {
			return
		}
	}
	return
}
