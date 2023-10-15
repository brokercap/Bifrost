package src

import "github.com/brokercap/Bifrost/plugin/driver"

func (c *Conn) CommitBatch(dataList []*driver.PluginDataType, retry bool) (LastSuccessCommitData *driver.PluginDataType, ErrData *driver.PluginDataType, err error) {
	var tmpLastSuccessCommitData *driver.PluginDataType
	c.p.BatchSize = len(dataList)
	// 从最后一个开始同步,因为假如同一个key的数据,在同一个批次的后面,则说明越新,后面执行的同一个key的数据,则可以直接丢弃
	c.p.Data = NewTableData()
	for i := range dataList {
		data := dataList[i]
		switch data.EventType {
		case "insert", "update", "delete":
			c.p.Data.Data = append(c.p.Data.Data, data)
		case "commit":
			c.p.Data.CommitData = append(c.p.Data.CommitData, data)
		case "sql":
			tmpLastSuccessCommitData, ErrData, err = c.AutoCommit()
			if tmpLastSuccessCommitData != nil {
				LastSuccessCommitData = tmpLastSuccessCommitData
			}
			if err != nil {
				return
			}
			c.p.Data = NewTableData()
			tmpLastSuccessCommitData, ErrData, err = c.Query(data, retry)
			if tmpLastSuccessCommitData != nil {
				LastSuccessCommitData = tmpLastSuccessCommitData
			}
			if err != nil {
				return
			}
		default:
			continue
		}
	}
	tmpLastSuccessCommitData, ErrData, err = c.AutoCommit()
	if tmpLastSuccessCommitData != nil {
		LastSuccessCommitData = tmpLastSuccessCommitData
	}
	if err != nil {
		return
	}
	return
}
