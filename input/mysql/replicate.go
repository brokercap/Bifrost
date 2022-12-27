package mysql

func (c *MysqlInput) AddReplicateDoDb(SchemaName,TableName string) (err error) {
	if c.binlogDump == nil {
		return
	}
	c.binlogDump.AddReplicateDoDb(SchemaName,TableName)
	return nil
}

func (c *MysqlInput) DelReplicateDoDb(SchemaName,TableName string) (err error) {
	if c.binlogDump == nil {
		return
	}
	c.binlogDump.DelReplicateDoDb(SchemaName,TableName)
	return nil
}