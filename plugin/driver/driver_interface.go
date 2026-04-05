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

// Filter 是可选过滤钩子，插件可按需实现字段动态增删等逻辑。
// keep=false 表示过滤掉当前事件；keep=true 且返回非nil data 表示继续同步。
func (c *PluginDriverInterface) Filter(data *PluginDataType, retry bool) (newData *PluginDataType, keep bool, err error) {
	return data, true, nil
}
