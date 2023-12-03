package mysql

func (c *MysqlInput) AddReplicateDoDb(SchemaName, TableName string) (err error) {
	c.AddReplicateDoDb0(SchemaName, TableName)
	if c.binlogDump == nil {
		return
	}
	c.binlogDump.AddReplicateDoDb(SchemaName, TableName)
	return nil
}

func (c *MysqlInput) AddReplicateDoDb0(SchemaName, TableName string) {
	c.Lock()
	defer c.Unlock()
	if c.replicateDoDb == nil {
		c.replicateDoDb = make(map[string]map[string]bool, 0)
	}
	if _, ok := c.replicateDoDb[SchemaName]; !ok {
		c.replicateDoDb[SchemaName] = make(map[string]bool, 0)
	}
	c.replicateDoDb[SchemaName][TableName] = true
	return
}

func (c *MysqlInput) DelReplicateDoDb(SchemaName, TableName string) (err error) {
	c.DelReplicateDoDb0(SchemaName, TableName)
	if c.binlogDump == nil {
		return
	}
	c.binlogDump.DelReplicateDoDb(SchemaName, TableName)
	return nil
}

func (c *MysqlInput) DelReplicateDoDb0(SchemaName, TableName string) {
	c.Lock()
	defer c.Unlock()
	if c.replicateDoDb == nil {
		return
	}
	if _, ok := c.replicateDoDb[SchemaName]; !ok {
		return
	}
	delete(c.replicateDoDb[SchemaName], TableName)
	return
}

func (c *MysqlInput) InitBinlogDumpReplicateDoDb() {
	c.Lock()
	defer c.Unlock()
	if c.replicateDoDb == nil {
		return
	}
	if c.binlogDump == nil {
		return
	}
	for schemaName, replicateDoDbTablesMap := range c.replicateDoDb {
		for tableName := range replicateDoDbTablesMap {
			c.binlogDump.AddReplicateDoDb(schemaName, tableName)
		}
	}
	return
}
