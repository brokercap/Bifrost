package src

import "github.com/brokercap/Bifrost/plugin/driver"

func (c *Conn) CommitBatch(dataList []*driver.PluginDataType, retry bool) (LastSuccessCommitData *driver.PluginDataType, ErrData *driver.PluginDataType, err error) {
	var tmpLastSuccessCommitData *driver.PluginDataType
	for i := range dataList {
		data := dataList[i]
		switch data.EventType {
		case "insert":
			tmpLastSuccessCommitData, ErrData, err = c.Insert(data, retry)
		case "update":
			tmpLastSuccessCommitData, ErrData, err = c.Update(data, retry)
		case "delete":
			tmpLastSuccessCommitData, ErrData, err = c.Del(data, retry)
		case "sql":
			tmpLastSuccessCommitData, ErrData, err = c.Query(data, retry)
		case "commit":
			tmpLastSuccessCommitData, ErrData, err = c.Commit(data, retry)
		default:
			continue
		}
		// 因为是正序排的,所以前面的位点,是被处理过的,这里将当前执行成功的位点,保存为最后一个成功执行的位点
		// 即使后面的循环中出错,也可以将最后一个成功的位点返回回去,
		// 这里和 set 的倒序执行,是相反的,需要注意,所以同时这里,即使后面循环中 Error了,也是不需要强制将  LastSuccessCommitData 设置为nil
		if tmpLastSuccessCommitData != nil {
			LastSuccessCommitData = tmpLastSuccessCommitData
		}
		if err != nil {
			return
		}
	}
	return
}
