package mysql

func (c *MysqlInput) AddReplicateDoDb(SchemaName,TableName string) (err error) {
	c.binlogDump.AddReplicateDoDb(SchemaName,TableName)
	return nil
}

func (c *MysqlInput) DelReplicateDoDb(SchemaName,TableName string) (err error) {
	c.binlogDump.DelReplicateDoDb(SchemaName,TableName)
	return nil
}