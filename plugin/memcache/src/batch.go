package src

import "github.com/brokercap/Bifrost/plugin/driver"

func (c *Conn) CommitBatch(dataList []*driver.PluginDataType, retry bool) (LastSuccessCommitData *driver.PluginDataType, ErrData *driver.PluginDataType, err error) {
	cacheMap := make(map[string]bool, 0)
	var ok bool
	var tmpLastSuccessCommitData *driver.PluginDataType
	// 从最后一个开始同步,因为假如同一个key的数据,在同一个批次的后面,则说明越新,后面执行的同一个key的数据,则可以直接丢弃
	for i := len(dataList) - 1; i >= 0; i-- {
		data := dataList[i]
		Key := c.getKeyVal(data, 0)
		if _, ok = cacheMap[Key]; ok {
			continue
		}
		cacheMap[Key] = true
		switch data.EventType {
		case "insert":
			tmpLastSuccessCommitData, ErrData, err = c.Update(data, retry)
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
		// 因为里倒序执行的,所以第一个位点,应该才是最后成功成功的数据位点
		if LastSuccessCommitData == nil && tmpLastSuccessCommitData != nil {
			LastSuccessCommitData = tmpLastSuccessCommitData
		}
		if err != nil {
			// 假如遇到的错误 ,则应该强制将最先成功,也即这一批最大的位点,给强制设置为nil,防止丢数据
			LastSuccessCommitData = nil
			return
		}
	}
	return
}
