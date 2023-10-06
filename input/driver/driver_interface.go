package driver

type PluginDriverInterface struct {
	replicateFitler *ReplicateFitler
}

func NewPluginDriverInterface() Driver {
	return &PluginDriverInterface{}
}

func (c *PluginDriverInterface) IsSupported(supportType SupportType) bool {
	switch supportType {
	case SupportFull, SupportIncre:
		return true
	}
	return false
}

func (c *PluginDriverInterface) SetOption(inputInfo InputInfo, param map[string]interface{}) {

}

func (c *PluginDriverInterface) GetUriExample() (string, string) {
	return "", ""
}

func (c *PluginDriverInterface) Start(ch chan *PluginStatus) error {
	return nil
}

func (c *PluginDriverInterface) Stop() error {
	return nil
}

func (c *PluginDriverInterface) Close() error {
	return nil
}

func (c *PluginDriverInterface) Kill() error {
	return nil
}

func (c *PluginDriverInterface) GetLastPosition() *PluginPosition {
	return nil
}

func (c *PluginDriverInterface) GetCurrentPosition() (*PluginPosition, error) {
	return nil, nil
}

func (c *PluginDriverInterface) Skip(skipEventCount int) error {
	return nil
}

func (c *PluginDriverInterface) SetEventID(eventId uint64) error {
	return nil
}

func (c *PluginDriverInterface) SetCallback(callback Callback) {

}

func (c *PluginDriverInterface) CheckPrivilege() error {
	return nil
}

func (c *PluginDriverInterface) CheckUri(CheckPrivilege bool) (CheckUriResult CheckUriResult, err error) {
	return
}

func (c *PluginDriverInterface) AddReplicateDoDb(SchemaName, TableName string) (err error) {
	return c.AddReplicateDoDb0(SchemaName, TableName)
}

func (c *PluginDriverInterface) DelReplicateDoDb(SchemaName, TableName string) (err error) {
	return c.DelReplicateDoDb0(SchemaName, TableName)
}

func (c *PluginDriverInterface) AddReplicateDoDb0(SchemaName, TableName string) (err error) {
	if c.replicateFitler == nil {
		c.replicateFitler = NewReplicateFitler()
	}
	c.replicateFitler.AddReplicateDoDb(SchemaName, TableName)
	return nil
}

func (c *PluginDriverInterface) DelReplicateDoDb0(SchemaName, TableName string) (err error) {
	if c.replicateFitler == nil {
		return
	}
	c.replicateFitler.DelReplicateDoDb(SchemaName, TableName)
	return nil
}

func (c *PluginDriverInterface) GetReplicateDoDbList() (databaseMapTableList map[string][]string) {
	return c.GetReplicateDoDbList0()
}

func (c *PluginDriverInterface) GetReplicateDoDbList0() (databaseMapTableList map[string][]string) {
	return c.replicateFitler.GetReplicateDoDbList()
}

func (c *PluginDriverInterface) CheckReplicateDb(SchemaName, TableName string) bool {
	if c.replicateFitler == nil {
		return true
	}
	return c.replicateFitler.CheckReplicateDb(SchemaName, TableName)
}

func (c *PluginDriverInterface) GetVersion() (string, error) {
	return "", nil
}

func (c *PluginDriverInterface) GetSchemaList() (SchemaList []string, err error) {
	return
}

func (c *PluginDriverInterface) GetSchemaTableList(schema string) (tableList []TableList, err error) {
	return
}

func (c *PluginDriverInterface) GetSchemaTableFieldList(schema string, table string) (FieldList []TableFieldInfo, err error) {
	return
}
func (c *PluginDriverInterface) DoneMinPosition(p *PluginPosition) (err error) {
	return
}
