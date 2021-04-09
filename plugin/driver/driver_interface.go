package driver

type ConnStatus int8

const (
	CLOSED  ConnStatus = 0
	STOPPED ConnStatus = 1
	RUNNING ConnStatus = 2
)

type PluginDriverInterface struct {
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
