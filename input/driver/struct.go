package driver

type InputInfo struct {
	IsGTID bool
	ConnectUri string
	GTID string
	BinlogFileName string
	BinlogPostion uint32
	ServerId uint32
	MaxFileName string
	MaxPosition uint32
}

type PluginStatus struct {
	Status StatusFlag
	Error  error
}

type PluginPosition struct {
	GTID string
	BinlogFileName string
	BinlogPostion uint32
	Timestamp uint32
	EventID	  uint64
}

type TableList struct {
	TableName string
	TableType string
}

type TableFieldInfo struct {
	ColumnName		  *string
	ColumnDefault	  *string
	IsNullable		  bool
	ColumnType		  *string
	IsAutoIncrement	 bool
	Comment		    *string
	DataType         *string
	NumericPrecision *uint64
	NumericScale	 *uint64
}

type CheckUriResult struct {
	BinlogFileName    string
	BinlogPostion int
	Gtid		   string
	ServerId       int
	Msg			[]string
}