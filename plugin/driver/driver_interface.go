package driver

type ConnStatus int8

const (
	CLOSED  ConnStatus = 0
	STOPPED ConnStatus = 1
	RUNNING ConnStatus = 2
)

type EventHandleFunc func(data *PluginDataType, retry bool) (*PluginDataType, *PluginDataType, error)

type PluginDriverInterface struct {
	childInsert EventHandleFunc
	childUpdate EventHandleFunc
	childDel    EventHandleFunc
	childQuery  EventHandleFunc
	childCommit EventHandleFunc
}

func (c *PluginDriverInterface) GetUriExample() string {
	return ""
}

func (c *PluginDriverInterface) SetOption(uri *string, param map[string]interface{}) {
	return
}

func (c *PluginDriverInterface) CheckUri() error {
	return nil
}

func (c *PluginDriverInterface) Open() error {
	return nil
}

func (c *PluginDriverInterface) Close() bool {
	return true
}

func (c *PluginDriverInterface) Insert(data *PluginDataType, retry bool) (LastSuccessCommitData *PluginDataType, ErrData *PluginDataType, err error) {
	return nil, nil, nil
}

func (c *PluginDriverInterface) Update(data *PluginDataType, retry bool) (LastSuccessCommitData *PluginDataType, ErrData *PluginDataType, err error) {
	return nil, nil, nil
}

func (c *PluginDriverInterface) Del(data *PluginDataType, retry bool) (LastSuccessCommitData *PluginDataType, ErrData *PluginDataType, err error) {
	return nil, nil, nil
}

func (c *PluginDriverInterface) Query(data *PluginDataType, retry bool) (LastSuccessCommitData *PluginDataType, ErrData *PluginDataType, err error) {
	return nil, nil, nil
}

func (c *PluginDriverInterface) Commit(data *PluginDataType, retry bool) (LastSuccessCommitData *PluginDataType, ErrData *PluginDataType, err error) {
	return data, nil, nil
}

func (c *PluginDriverInterface) SetParam(p interface{}) (interface{}, error) {
	return nil, nil
}

func (c *PluginDriverInterface) TimeOutCommit() (LastSuccessCommitData *PluginDataType, ErrData *PluginDataType, err error) {
	return nil, nil, nil
}

func (c *PluginDriverInterface) Skip(SkipData *PluginDataType) error {
	return nil
}

func (c *PluginDriverInterface) CommitBatch(dataList []*PluginDataType, retry bool) (LastSuccessCommitData *PluginDataType, ErrData *PluginDataType, err error) {
	return c.AscCommitBatch(dataList, retry)
}

func (c *PluginDriverInterface) CommitBatchReturnLastSuccessCommitData(dataList []*PluginDataType, retry bool) (LastSuccessCommitData *PluginDataType, ErrData *PluginDataType, err error) {
	for i := len(dataList) - 1; i >= 0; i-- {
		data := dataList[i]
		switch data.EventType {
		case "commit":
			return data, nil, nil
		default:
			continue
		}
	}
	return
}

func (c *PluginDriverInterface) AscCommitBatch(dataList []*PluginDataType, retry bool) (LastSuccessCommitData *PluginDataType, ErrData *PluginDataType, err error) {
	var tmpLastSuccessCommitData *PluginDataType
	for i := range dataList {
		data := dataList[i]
		switch data.EventType {
		case "insert":
			if c.childInsert != nil {
				LastSuccessCommitData, ErrData, err = c.childInsert(data, retry)
			}
		case "update":
			if c.childInsert != nil {
				LastSuccessCommitData, ErrData, err = c.childUpdate(data, retry)
			}
		case "delete":
			if c.childInsert != nil {
				LastSuccessCommitData, ErrData, err = c.childDel(data, retry)
			}
		case "sql":
			if c.childInsert != nil {
				LastSuccessCommitData, ErrData, err = c.childQuery(data, retry)
			}
		case "commit":
			if c.childInsert != nil {
				LastSuccessCommitData, ErrData, err = c.childCommit(data, retry)
			}
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

func (c *PluginDriverInterface) SetChildInsertFunc(f EventHandleFunc) {
	c.childInsert = f
}

func (c *PluginDriverInterface) SetChildUpdateFunc(f EventHandleFunc) {
	c.childUpdate = f
}

func (c *PluginDriverInterface) SetChildDelFunc(f EventHandleFunc) {
	c.childDel = f
}

func (c *PluginDriverInterface) SetChildQueryFunc(f EventHandleFunc) {
	c.childQuery = f
}

func (c *PluginDriverInterface) SetChildCommitFunc(f EventHandleFunc) {
	c.childCommit = f
}
