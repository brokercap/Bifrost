package mysql

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"log"
	"runtime/debug"
	"strings"
)

type eventParser struct {
	format                *FormatDescriptionEvent
	tableMap              map[uint64]*TableMapEvent // tableId 对应的最后一个 TableMapEvent 事件
	tableNameMap          map[string]uint64         // schame.table 做为key 对应的tableId
	tableSchemaMap        map[uint64]*tableStruct   // tableId 对应的表结构
	dataSource            *string
	connStatus            StatusFlag
	conn                  MysqlConnection
	dumpBinLogStatus      StatusFlag
	binlogFileName        string
	currentBinlogFileName string
	binlogPosition        uint32
	binlogTimestamp       uint32
	lastEventID           uint64
	maxBinlogFileName     string
	maxBinlogPosition     uint32
	eventDo               []bool
	ServerId              uint32
	connectionId          string
	binlog_checksum       bool
	filterNextRowEvent    bool
	binlogDump            *BinlogDump
	lastMapEvent          *TableMapEvent // 保存最近一次 map event 解析出来的 tableId，用于接下来的 row event 解析使用，因为实际运行中发现，row event 解析出来 tableId 可能对不上。row event 紧跟在 map event 之后，row event 的时候，直接采用最后一次map event
	callbackErrChan       chan error
	isGTID                bool
	nextEventID           uint64               // 下一个事件ID, 不能修改
	lastPrevtiousGTIDSMap map[string]Intervals // 当前解析的 binlog 文件的 PrevtiousGTIDS 对应关系
	gtidSetInfo           GTIDSet
	dbType                DBType
}

func newEventParser(binlogDump *BinlogDump) (parser *eventParser) {
	parser = new(eventParser)
	parser.tableMap = make(map[uint64]*TableMapEvent)
	parser.tableNameMap = make(map[string]uint64)
	parser.tableSchemaMap = make(map[uint64]*tableStruct)
	parser.eventDo = make([]bool, 164, 164)
	parser.ServerId = 21036
	parser.connectionId = ""
	parser.maxBinlogFileName = ""
	parser.maxBinlogPosition = 0
	parser.binlog_checksum = false
	parser.filterNextRowEvent = false
	parser.binlogDump = binlogDump
	return
}

func (parser *eventParser) getNextEventID() uint64 {
	parser.nextEventID += 1
	return parser.nextEventID
}

func (parser *eventParser) getGTIDSIDStart(sid string) int64 {
	if _, ok := parser.lastPrevtiousGTIDSMap[sid]; ok {
		return parser.lastPrevtiousGTIDSMap[sid].Start
	}
	return 1
}

func (parser *eventParser) saveBinlog(event *EventReslut) {
	switch event.Header.EventType {
	case QUERY_EVENT, XID_EVENT:
		if event.BinlogFileName == "" {
			return
		}
		parser.binlogDump.Lock()
		parser.binlogFileName = event.BinlogFileName
		parser.binlogPosition = event.Header.LogPos
		parser.binlogTimestamp = event.Header.Timestamp
		parser.lastEventID = event.EventID
		parser.binlogDump.Unlock()
		break
	case ROTATE_EVENT:
		parser.binlogDump.Lock()
		parser.currentBinlogFileName = event.BinlogFileName
		parser.lastEventID = event.EventID
		parser.binlogDump.Unlock()
	case GTID_EVENT, ANONYMOUS_GTID_EVENT, MARIADB_GTID_EVENT:
		parser.binlogDump.Lock()
		parser.binlogTimestamp = event.Header.Timestamp
		parser.lastEventID = event.EventID
		parser.binlogDump.Unlock()
		break
	default:
		break
	}
}

func (parser *eventParser) getGtid() string {
	if parser.gtidSetInfo == nil {
		return ""
	}
	return parser.gtidSetInfo.String()
}

func (parser *eventParser) parseEvent(data []byte) (event *EventReslut, filename string, err error) {
	var buf *bytes.Buffer
	if parser.binlog_checksum {
		buf = bytes.NewBuffer(data[0 : len(data)-4])
	} else {
		buf = bytes.NewBuffer(data)
	}
	//log.Println("data[4]:",data[4])
	switch EventType(data[4]) {
	case HEARTBEAT_EVENT, IGNORABLE_EVENT:
		return
	case PREVIOUS_GTIDS_EVENT:
		var PreviousGTIDSEvent *PreviousGTIDSEvent
		PreviousGTIDSEvent, err = parser.parsePrevtiousGTIDSEvent(buf)
		event = &EventReslut{
			Header:         PreviousGTIDSEvent.header,
			BinlogFileName: parser.currentBinlogFileName,
			BinlogPosition: PreviousGTIDSEvent.header.LogPos,
		}
		return
	case GTID_EVENT, ANONYMOUS_GTID_EVENT:
		var GtidEvent *GTIDEvent
		GtidEvent, err = parser.parseGTIDEvent(buf)
		gtid := fmt.Sprintf("%s:%d-%d", GtidEvent.SID36, parser.getGTIDSIDStart(GtidEvent.SID36), GtidEvent.GNO)
		parser.gtidSetInfo.Update(gtid)
		event = &EventReslut{
			Header:         GtidEvent.header,
			BinlogFileName: parser.currentBinlogFileName,
			BinlogPosition: GtidEvent.header.LogPos,
			Gtid:           parser.gtidSetInfo.String(),
		}
		break
	case MARIADB_GTID_LIST_EVENT:
		var MariaDBGTIDSEvent *MariadbGTIDListEvent
		MariaDBGTIDSEvent, err = parser.MariadbGTIDListEvent(buf)
		event = &EventReslut{
			Header:         MariaDBGTIDSEvent.header,
			BinlogFileName: parser.currentBinlogFileName,
			BinlogPosition: MariaDBGTIDSEvent.header.LogPos,
		}
		return
	case MARIADB_GTID_EVENT:
		var GtidEvent *MariadbGTIDEvent
		GtidEvent, err = parser.MariadbGTIDEvent(buf)
		gtid := fmt.Sprintf("%d-%d-%d", GtidEvent.GTID.DomainID, GtidEvent.GTID.ServerID, GtidEvent.GTID.SequenceNumber)
		parser.gtidSetInfo.Update(gtid)
		event = &EventReslut{
			Header:         GtidEvent.header,
			BinlogFileName: parser.currentBinlogFileName,
			BinlogPosition: GtidEvent.header.LogPos,
			Gtid:           parser.gtidSetInfo.String(),
		}
		return
	case FORMAT_DESCRIPTION_EVENT:
		parser.format, err = parser.parseFormatDescriptionEvent(buf)
		if strings.Contains(parser.format.mysqlServerVersion, "MariaDB") {
			parser.dbType = DB_TYPE_MARIADB
		}
		// 这要地方要对 gtidSetInfo 初始化，在假如非 GTID 解析的情况下，但是 数据库本身又有 GTID 事件，是存在可能解析出错的情况的
		if parser.gtidSetInfo == nil {
			if parser.dbType == DB_TYPE_MARIADB {
				parser.gtidSetInfo = NewMariaDBGtidSet("")
			} else {
				parser.gtidSetInfo = NewMySQLGtidSet("")
			}
		}
		/*
			i := strings.IndexAny(parser.format.mysqlServerVersion, "-")
			var version string
			if i> 0{
				version = parser.format.mysqlServerVersion[0:i]
			}else{
				version = parser.format.mysqlServerVersion
			}
			if len(version)==5{
				version = strings.Replace(version, ".", "", 1)
				version = strings.Replace(version, ".", "0", 1)
			}else{
				version = strings.Replace(version, ".", "", -1)
			}
			parser.mysqlVersionInt,err = strconv.Atoi(version)
			if err != nil{
				log.Println("mysql version:",version,"err",err)
			}
		*/
		//log.Println("binlogVersion:",parser.format.binlogVersion,"server version:",parser.format.mysqlServerVersion)
		event = &EventReslut{
			Header: parser.format.header,
		}
		return
	case QUERY_EVENT:
		var queryEvent *QueryEvent
		queryEvent, err = parser.parseQueryEvent(buf)
		event = &EventReslut{
			Header:         queryEvent.header,
			SchemaName:     queryEvent.schema,
			BinlogFileName: parser.currentBinlogFileName,
			TableName:      "",
			Query:          queryEvent.query,
			BinlogPosition: queryEvent.header.LogPos,
		}
		switch queryEvent.query {
		case "COMMIT":
			event.Gtid = parser.getGtid()
		default:
			break
		}
		break
	case ROTATE_EVENT:
		var rotateEvent *RotateEvent
		rotateEvent, err = parser.parseRotateEvent(buf)
		event = &EventReslut{
			Header:         rotateEvent.header,
			BinlogFileName: rotateEvent.filename,
			BinlogPosition: rotateEvent.header.LogPos,
		}
		for _, v := range parser.tableSchemaMap {
			v.needReload = true
		}
		parser.saveBinlog(event)
		log.Println(*parser.dataSource, " ROTATE_EVENT ", event.BinlogFileName)
		break
	case TABLE_MAP_EVENT:
		var table_map_event *TableMapEvent
		table_map_event, err = parser.parseTableMapEvent(buf)
		//log.Println("table_map_event:",table_map_event)
		parser.tableMap[table_map_event.tableId] = table_map_event
		parser.lastMapEvent = table_map_event
		//log.Println("table_map_event:",*table_map_event,"tableId:",table_map_event.tableId," schemaName:",table_map_event.schemaName," tableName:",table_map_event.tableName)
		if parser.binlogDump.CheckReplicateDb(table_map_event.schemaName, table_map_event.tableName) == false {
			parser.filterNextRowEvent = true
		} else {
			parser.filterNextRowEvent = false
			_, ok := parser.tableSchemaMap[table_map_event.tableId]
			if !ok || (parser.tableSchemaMap[table_map_event.tableId].needReload == true) {
				parser.GetTableSchema(table_map_event.tableId, table_map_event.schemaName, table_map_event.tableName)
			}
		}
		event = &EventReslut{
			Header:         table_map_event.header,
			BinlogFileName: parser.currentBinlogFileName,
			BinlogPosition: table_map_event.header.LogPos,
			SchemaName:     parser.tableMap[table_map_event.tableId].schemaName,
			TableName:      parser.tableMap[table_map_event.tableId].tableName,
		}

		break
	case WRITE_ROWS_EVENTv0, WRITE_ROWS_EVENTv1, WRITE_ROWS_EVENTv2, UPDATE_ROWS_EVENTv0, UPDATE_ROWS_EVENTv1, UPDATE_ROWS_EVENTv2, DELETE_ROWS_EVENTv0, DELETE_ROWS_EVENTv1, DELETE_ROWS_EVENTv2:
		var rowsEvent *RowsEvent
		rowsEvent, err = parser.parseRowsEvent(buf)
		if err != nil {
			log.Println("row event err:", err)
		}
		if tableInfo, ok := parser.tableSchemaMap[rowsEvent.tableId]; ok {
			event = &EventReslut{
				Header:         rowsEvent.header,
				BinlogFileName: parser.currentBinlogFileName,
				BinlogPosition: rowsEvent.header.LogPos,
				SchemaName:     tableInfo.SchemaName,
				TableName:      tableInfo.TableName,
				Rows:           rowsEvent.rows,
				Pri:            tableInfo.Pri,
				ColumnMapping:  tableInfo.ColumnMapping,
			}
		} else {
			event = &EventReslut{
				Header:         rowsEvent.header,
				BinlogFileName: parser.currentBinlogFileName,
				BinlogPosition: rowsEvent.header.LogPos,
				SchemaName:     parser.lastMapEvent.schemaName,
				TableName:      parser.lastMapEvent.tableName,
				Rows:           rowsEvent.rows,
			}
		}
		break
	case XID_EVENT:
		var xidEvent *XIdEvent
		xidEvent, err = parser.parseXidEvent(buf)
		if err != nil {
			log.Println("xid event err:", err)
		}
		event = &EventReslut{
			Header:         xidEvent.header,
			BinlogFileName: parser.currentBinlogFileName,
			BinlogPosition: xidEvent.header.LogPos,
			SchemaName:     "",
			TableName:      "",
			Rows:           nil,
			Gtid:           parser.gtidSetInfo.String(),
		}
		break
	default:
		var genericEvent *GenericEvent
		genericEvent, err = parseGenericEvent(buf)
		event = &EventReslut{
			Header: genericEvent.header,
		}
		event.BinlogFileName = parser.currentBinlogFileName
		event.BinlogPosition = genericEvent.header.LogPos
	}
	return
}

// 初始化 用于查询mysql 表结构等信息的连接，这个不是 主从连接
func (parser *eventParser) initConn() {
	dbopen := &mysqlDriver{}
	conn, err := dbopen.Open(*parser.dataSource)
	if err != nil {
		panic(err)
	} else {
		parser.connStatus = STATUS_RUNNING
	}
	parser.conn = conn.(MysqlConnection)
}

// 关闭用于查询mysql 表结构等信息的连接
func (parser *eventParser) ParserConnClose(lock bool) {
	if lock == true {
		parser.binlogDump.Lock()
		defer parser.binlogDump.Unlock()
	}
	parser.connStatus = STATUS_CLOSED
	if parser.conn != nil {
		func() {
			func() {
				if err := recover(); err != nil {
					return
				}
			}()
			parser.conn.Close()
		}()
		parser.conn = nil
	}
}

func (parser *eventParser) GetTableSchema(tableId uint64, database string, tablename string) {
	//var errPrint bool = false
	var lastErr string
	for {
		err := parser.GetTableSchemaByName(tableId, database, tablename)
		if err == nil {
			break
		} else {
			if lastErr != err.Error() {
				log.Println("binlog GetTableSchema err:", err, " tableId:", tableId, " database:", database, " tablename:", tablename)
				lastErr = err.Error()
			}
		}
	}
}

func (parser *eventParser) GetTableSchemaByName(tableId uint64, database string, tablename string) (errs error) {
	parser.binlogDump.Lock()
	defer parser.binlogDump.Unlock()
	errs = fmt.Errorf("unknow error")
	defer func() {
		if err := recover(); err != nil {
			parser.ParserConnClose(false)
			errs = fmt.Errorf(string(debug.Stack()))
		}
	}()
	if parser.connStatus == STATUS_CLOSED {
		parser.initConn()
	}
	//set dbAndTable Name tableId
	parser.tableNameMap[database+"."+tablename] = tableId
	sql := "SELECT COLUMN_NAME,COLUMN_KEY,COLUMN_TYPE,CHARACTER_SET_NAME,COLLATION_NAME,NUMERIC_SCALE,EXTRA,COLUMN_DEFAULT,DATA_TYPE,CHARACTER_OCTET_LENGTH,IS_NULLABLE FROM information_schema.columns WHERE table_schema='" + database + "' AND table_name='" + tablename + "' ORDER BY `ORDINAL_POSITION` ASC"
	stmt, err := parser.conn.Prepare(sql)
	if err != nil {
		errs = err
		parser.ParserConnClose(false)
		return
	}
	defer stmt.Close()
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	if err != nil {
		errs = err
		parser.ParserConnClose(false)
		return
	}
	defer rows.Close()
	//columeArr := make([]*tableStruct column_schema_type,0)
	tableInfo := &tableStruct{
		SchemaName:           database,
		TableName:            tablename,
		Pri:                  make([]string, 0),
		ColumnSchemaTypeList: make([]*ColumnInfo, 0),
	}
	ColumnMapping := make(map[string]string, 0)
	for {
		dest := make([]driver.Value, 11, 11)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		var COLUMN_NAME, COLUMN_KEY, COLUMN_TYPE string
		var CHARACTER_SET_NAME, COLLATION_NAME, NUMERIC_SCALE, EXTRA string
		var isBool bool = false
		var unsigned bool = false
		var is_primary bool = false
		var auto_increment bool = false
		var enum_values, set_values []string
		var COLUMN_DEFAULT string
		var DATA_TYPE string
		var CHARACTER_OCTET_LENGTH uint64
		var IS_NULLABLE string

		COLUMN_NAME = dest[0].(string)
		COLUMN_KEY = dest[1].(string)
		COLUMN_TYPE = dest[2].(string)
		if dest[3] == nil {
			CHARACTER_SET_NAME = ""
		} else {
			CHARACTER_SET_NAME = dest[3].(string)
		}

		if dest[4] == nil {
			COLLATION_NAME = ""
		} else {
			COLLATION_NAME = dest[4].(string)
		}

		if dest[5] == nil {
			NUMERIC_SCALE = ""
		} else {
			NUMERIC_SCALE = fmt.Sprint(dest[5])
		}

		EXTRA = dest[6].(string)

		DATA_TYPE = dest[8].(string)

		//bit类型这个地方比较特殊，不能直接转成string，并且当前只有 time,datetime 类型转换的时候会用到 默认值，这里不进行其他细节处理
		if DATA_TYPE != "bit" {
			if dest[7] == nil {
				COLUMN_DEFAULT = ""
			} else {
				COLUMN_DEFAULT = dest[7].(string)
			}
		}

		if COLUMN_TYPE == "tinyint(1)" {
			isBool = true
		}
		if EXTRA == "auto_increment" {
			auto_increment = true
		}
		if strings.Contains(COLUMN_TYPE, "unsigned") {
			unsigned = true
		}
		if COLUMN_KEY != "" {
			is_primary = true
		}

		if DATA_TYPE == "enum" {
			d := strings.Replace(COLUMN_TYPE, "enum(", "", -1)
			d = strings.Replace(d, ")", "", -1)
			d = strings.Replace(d, "'", "", -1)
			enum_values = strings.Split(d, ",")
		} else {
			enum_values = make([]string, 0)
		}

		if DATA_TYPE == "set" {
			d := strings.Replace(COLUMN_TYPE, "set(", "", -1)
			d = strings.Replace(d, ")", "", -1)
			d = strings.Replace(d, "'", "", -1)
			set_values = strings.Split(d, ",")
		} else {
			set_values = make([]string, 0)
		}

		if dest[9] == nil {
			CHARACTER_OCTET_LENGTH = 0
		} else {
			switch dest[9].(type) {
			case uint32:
				CHARACTER_OCTET_LENGTH = uint64(dest[9].(uint32))
			case uint64:
				CHARACTER_OCTET_LENGTH = dest[9].(uint64)
			case int64:
				CHARACTER_OCTET_LENGTH = uint64(dest[9].(int64))
			case int32:
				CHARACTER_OCTET_LENGTH = uint64(dest[9].(int32))

			default:
				CHARACTER_OCTET_LENGTH = 0
			}
		}
		if dest[10] == nil {
			IS_NULLABLE = "YES"
		} else {
			IS_NULLABLE = dest[10].(string)
		}

		tableInfo.ColumnSchemaTypeList = append(tableInfo.ColumnSchemaTypeList, &ColumnInfo{
			COLUMN_NAME:            COLUMN_NAME,
			COLUMN_KEY:             COLUMN_KEY,
			COLUMN_TYPE:            COLUMN_TYPE,
			EnumValues:             enum_values,
			SetValues:              set_values,
			IsBool:                 isBool,
			Unsigned:               unsigned,
			IsPrimary:              is_primary,
			AutoIncrement:          auto_increment,
			CHARACTER_SET_NAME:     CHARACTER_SET_NAME,
			COLLATION_NAME:         COLLATION_NAME,
			NUMERIC_SCALE:          NUMERIC_SCALE,
			COLUMN_DEFAULT:         COLUMN_DEFAULT,
			DATA_TYPE:              DATA_TYPE,
			CHARACTER_OCTET_LENGTH: CHARACTER_OCTET_LENGTH,
		})

		if strings.ToUpper(COLUMN_KEY) == "PRI" {
			tableInfo.Pri = append(tableInfo.Pri, COLUMN_NAME)
		}

		var columnMappingType string
		switch DATA_TYPE {
		case "tinyint":
			if unsigned {
				columnMappingType = "uint8"
			} else {
				if COLUMN_TYPE == "tinyint(1)" {
					columnMappingType = "bool"
				} else {
					columnMappingType = "int8"
				}
			}
		case "smallint":
			if unsigned {
				columnMappingType = "uint16"
			} else {
				columnMappingType = "int16"
			}
		case "mediumint":
			if unsigned {
				columnMappingType = "uint24"
			} else {
				columnMappingType = "int24"
			}
		case "int":
			if unsigned {
				columnMappingType = "uint32"
			} else {
				columnMappingType = "int32"
			}
		case "bigint":
			if unsigned {
				columnMappingType = "uint64"
			} else {
				columnMappingType = "int64"
			}
		case "numeric":
			columnMappingType = strings.Replace(COLUMN_TYPE, "numeric", "decimal", 1)
		case "real":
			columnMappingType = strings.Replace(COLUMN_TYPE, "real", "double", 1)
		default:
			columnMappingType = COLUMN_TYPE
			break
		}
		if IS_NULLABLE == "YES" {
			columnMappingType = "Nullable(" + columnMappingType + ")"
		}
		ColumnMapping[COLUMN_NAME] = columnMappingType
	}
	if len(tableInfo.ColumnSchemaTypeList) == 0 {
		return fmt.Errorf("column len is 0 " + "db:" + database + " table:" + tablename + " tableId:" + fmt.Sprint(tableId) + " may be no privilege")
	}
	tableInfo.needReload = false
	tableInfo.ColumnMapping = ColumnMapping
	parser.tableSchemaMap[tableId] = tableInfo
	errs = nil
	return
}

func (parser *eventParser) GetConnectionInfo(connectionId string) (m map[string]string, e error) {
	parser.binlogDump.Lock()
	defer func() {
		if err := recover(); err != nil {
			parser.ParserConnClose(false)
			log.Println("binlog.go GetConnectionInfo err:", err)
			m = nil
		}
		parser.binlogDump.Unlock()
	}()
	if parser.connStatus == STATUS_CLOSED {
		parser.initConn()
	}
	sql := "select TIME,STATE from `information_schema`.`PROCESSLIST` WHERE ID='" + connectionId + "'"
	stmt, err := parser.conn.Prepare(sql)
	if err != nil {
		parser.ParserConnClose(false)
		return nil, nil
	}
	defer stmt.Close()
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	if err != nil {
		parser.ParserConnClose(false)
		return nil, err
	}
	defer rows.Close()
	m = make(map[string]string, 2)
	for {
		dest := make([]driver.Value, 2, 2)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		m["TIME"] = fmt.Sprint(dest[0])
		m["STATE"] = dest[1].(string)
		break
	}
	return m, nil
}

func (parser *eventParser) KillConnect(connectionId string) (b bool) {
	if connectionId == "" {
		return true
	}
	b = false
	parser.binlogDump.Lock()
	defer func() {
		if err := recover(); err != nil {
			parser.ParserConnClose(false)
			b = false
		}
		parser.binlogDump.Unlock()
	}()
	if parser.connStatus == STATUS_CLOSED {
		parser.initConn()
	}
	sql := "kill " + connectionId
	_, err := parser.conn.Exec(sql, []driver.Value{})
	if err != nil {
		parser.ParserConnClose(false)
		return false
	}
	return true
}

func (parser *eventParser) GetTableId(database string, tablename string) (uint64, error) {
	key := database + "." + tablename
	if _, ok := parser.tableNameMap[key]; !ok {
		return 0, fmt.Errorf("not found key:%s", key)
	}
	return parser.tableNameMap[key], nil
}

func (parser *eventParser) delTableId(database string, tablename string) {
	key := database + "." + tablename
	if tableId, ok := parser.tableNameMap[key]; ok {
		delete(parser.tableSchemaMap, tableId)
	}
	delete(parser.tableNameMap, key)
	return
}
