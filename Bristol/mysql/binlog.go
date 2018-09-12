package mysql

import (
	"bytes"
	"database/sql/driver"

	"log"
	"strings"

	"encoding/hex"
	"fmt"
	"time"
	"sync"
)

type eventParser struct {
	format           *FormatDescriptionEvent
	tableMap         map[uint64]*TableMapEvent
	tableNameMap     map[string]uint64
	tableSchemaMap   map[uint64][]*column_schema_type
	dataSource       *string
	connStatus       int8 //0 stop  1 running
	conn             MysqlConnection
	dumpBinLogStatus uint8 //0 stop ,1 running
	binlogFileName   string
	binlogPosition   uint32
	maxBinlogFileName   string
	maxBinlogPosition   uint32
	binlogIgnoreDb   *string
	replicateDoDb    map[string]uint8
	eventDo          []bool
	ServerId         uint32
	connectionId	 string
	connLock 		 sync.Mutex
	binlog_checksum  bool
}

func newEventParser() (parser *eventParser) {
	parser = new(eventParser)
	parser.tableMap = make(map[uint64]*TableMapEvent)
	parser.tableNameMap = make(map[string]uint64)
	parser.tableSchemaMap = make(map[uint64][]*column_schema_type)
	parser.eventDo = make([]bool, 36, 36)
	parser.ServerId = 1
	parser.connectionId = ""
	parser.maxBinlogFileName = ""
	parser.maxBinlogPosition = 0
	parser.binlog_checksum = false
	return
}

func (parser *eventParser) parseEvent(data []byte) (event *EventReslut, filename string, err error) {
	var buf *bytes.Buffer
	if parser.binlog_checksum {
		buf = bytes.NewBuffer(data[0:len(data)-4])
	}else{
		buf = bytes.NewBuffer(data)
	}

	filename = parser.binlogFileName
	switch EventType(data[4]) {
	case HEARTBEAT_EVENT,IGNORABLE_EVENT,GTID_EVENT,ANONYMOUS_GTID_EVENT,PREVIOUS_GTIDS_EVENT:
		return
	case FORMAT_DESCRIPTION_EVENT:
		parser.format, err = parser.parseFormatDescriptionEvent(buf)
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
			BinlogFileName: parser.binlogFileName,
			TableName:      "",
			Query:          queryEvent.query,
		}
		return
	case ROTATE_EVENT:
		var rotateEvent *RotateEvent
		rotateEvent, err = parser.parseRotateEvent(buf)
		parser.binlogFileName = rotateEvent.filename
		parser.binlogPosition = uint32(rotateEvent.position)
		filename = parser.binlogFileName
		event = &EventReslut{
			Header:         rotateEvent.header,
			BinlogFileName: parser.binlogFileName,
			BinlogPosition: parser.binlogPosition,
		}
		parser.tableSchemaMap = make(map[uint64][]*column_schema_type,0)
		return
	case TABLE_MAP_EVENT:
		var table_map_event *TableMapEvent
		table_map_event, err = parser.parseTableMapEvent(buf)
		parser.tableMap[table_map_event.tableId] = table_map_event
		if _, ok := parser.tableSchemaMap[table_map_event.tableId]; !ok {
			parser.GetTableSchema(table_map_event.tableId, table_map_event.schemaName, table_map_event.tableName)
		}
		event = &EventReslut{
			Header:         table_map_event.header,
			BinlogFileName: parser.binlogFileName,
			BinlogPosition: parser.binlogPosition,
			SchemaName:     parser.tableMap[table_map_event.tableId].schemaName,
			TableName:      parser.tableMap[table_map_event.tableId].tableName,
		}
		return
	case WRITE_ROWS_EVENTv0,WRITE_ROWS_EVENTv1,WRITE_ROWS_EVENTv2, UPDATE_ROWS_EVENTv0,UPDATE_ROWS_EVENTv1,UPDATE_ROWS_EVENTv2, DELETE_ROWS_EVENTv0,DELETE_ROWS_EVENTv1,DELETE_ROWS_EVENTv2:
		var rowsEvent *RowsEvent
		rowsEvent, err = parser.parseRowsEvent(buf)
		if err != nil{
			log.Println("row event err:",err)
		}
		event = &EventReslut{
			Header:         rowsEvent.header,
			BinlogFileName: parser.binlogFileName,
			BinlogPosition: parser.binlogPosition,
			SchemaName:     parser.tableMap[rowsEvent.tableId].schemaName,
			TableName:      parser.tableMap[rowsEvent.tableId].tableName,
			Rows:           rowsEvent.rows,
		}
	default:
		var genericEvent *GenericEvent
		genericEvent, err = parseGenericEvent(buf)
		event = &EventReslut{
			Header: genericEvent.header,
		}
	}
	return
}

func (parser *eventParser) initConn() {
	dbopen := &mysqlDriver{}
	conn, err := dbopen.Open(*parser.dataSource)
	if err != nil {
		panic(err)
	} else {
		parser.connStatus = 1
	}
	parser.conn = conn.(MysqlConnection)
}

func (parser *eventParser) GetTableSchema(tableId uint64, database string, tablename string) {
	for {
		parser.connLock.Lock()
		err := parser.GetTableSchemaByName(tableId,database,tablename)
		parser.connLock.Unlock()
		if err == nil{
			break
		}
	}
}

func (parser *eventParser) GetTableSchemaByName(tableId uint64, database string, tablename string) (errs error) {
	errs = fmt.Errorf("unknow error")
	defer func() {
		if err := recover(); err != nil {
			if parser.connStatus == 1{
				parser.connStatus = 0
				parser.conn.Close()
			}
			errs = fmt.Errorf(fmt.Sprint(err))
		}
	}()
	if parser.connStatus == 0 {
		parser.initConn()
	}
	//set dbAndTable Name tableId
	parser.tableNameMap[database+"."+tablename] = tableId
	sql := "SELECT COLUMN_NAME,COLUMN_KEY,COLUMN_TYPE,CHARACTER_SET_NAME,COLLATION_NAME,NUMERIC_SCALE,EXTRA FROM information_schema.columns WHERE table_schema='" + database + "' AND table_name='" + tablename + "' ORDER BY `ORDINAL_POSITION` ASC"
	stmt, err := parser.conn.Prepare(sql)
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	if err != nil {
		errs = err
		return
	}
	for {
		dest := make([]driver.Value, 7, 7)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		var COLUMN_NAME, COLUMN_KEY, COLUMN_TYPE string
		var CHARACTER_SET_NAME,COLLATION_NAME,NUMERIC_SCALE,EXTRA string
		var isBool bool = false
		var unsigned bool = false
		var is_primary bool = false
		var auto_increment bool = false
		var enum_values, set_values []string

		COLUMN_NAME = string(dest[0].([]byte))
		COLUMN_KEY = string(dest[1].([]byte))
		COLUMN_TYPE = string(dest[2].([]byte))
		CHARACTER_SET_NAME = string(dest[3].([]byte))
		COLLATION_NAME = string(dest[4].([]byte))
		NUMERIC_SCALE = string(dest[5].([]byte))
		EXTRA = string(dest[6].([]byte))
		if COLUMN_TYPE == "tinyint(1)"{
			isBool = true
		}
		if EXTRA == "auto_increment"{
			auto_increment = true
		}
		if strings.Contains(COLUMN_TYPE,"unsigned"){
			unsigned = true
		}
		if COLUMN_KEY != ""{
			is_primary = true
		}

		if COLUMN_TYPE[0:4] == "enum" {
			d := strings.Replace(COLUMN_TYPE, "enum(", "", -1)
			d = strings.Replace(d, ")", "", -1)
			d = strings.Replace(d, "'", "", -1)
			enum_values = strings.Split(d, ",")
		} else {
			enum_values = make([]string, 0)
		}

		if COLUMN_TYPE[0:3] == "set" {
			d := strings.Replace(COLUMN_TYPE, "set(", "", -1)
			d = strings.Replace(d, ")", "", -1)
			d = strings.Replace(d, "'", "", -1)
			set_values = strings.Split(d, ",")
		} else {
			set_values = make([]string, 0)
		}
		parser.tableSchemaMap[tableId] = append(parser.tableSchemaMap[tableId], &column_schema_type{
			COLUMN_NAME: COLUMN_NAME,
			COLUMN_KEY:  COLUMN_KEY,
			COLUMN_TYPE: COLUMN_TYPE,
			enum_values: enum_values,
			set_values:  set_values,
			is_bool:	 isBool,
			unsigned:    unsigned,
			is_primary:  is_primary,
			auto_increment: auto_increment,
			CHARACTER_SET_NAME:CHARACTER_SET_NAME,
			COLLATION_NAME:COLLATION_NAME,
			NUMERIC_SCALE:NUMERIC_SCALE,
		})
	}
	rows.Close()
	errs = nil
	return
}

func (parser *eventParser) GetConnectionInfo(connectionId string) (m map[string]string){
	parser.connLock.Lock()
	defer func() {
		if err := recover(); err != nil {
			if parser.connStatus == 1{
				parser.connStatus = 0
				parser.conn.Close()
			}
			parser.connLock.Unlock()
			log.Println("binlog.go GetConnectionInfo err:",err)
			m = nil
		}else{
			parser.connLock.Unlock()
		}
	}()
	if parser.connStatus == 0 {
		parser.initConn()
	}
	sql := "select TIME,STATE from `information_schema`.`PROCESSLIST` WHERE ID='"+connectionId+"'"
	stmt, err := parser.conn.Prepare(sql)
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	if err != nil {
		return nil
	}
	m = make(map[string]string,2)
	for {
		dest := make([]driver.Value, 2, 2)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		m["TIME"] = string(dest[0].([]byte))
		m["STATE"]=string(dest[1].([]byte))
		break
	}
	return
}


func (parser *eventParser) KillConnect(connectionId string) (b bool){
	b = false
	parser.connLock.Lock()
	defer func() {
		if err := recover(); err != nil {
			if parser.connStatus == 1{
				parser.connStatus = 0
				parser.conn.Close()
			}
			parser.connLock.Unlock()
			b = false
		}else{
			parser.connLock.Unlock()
		}
	}()
	if parser.connStatus == 0 {
		parser.initConn()
	}
	sql := "kill "+connectionId
	p := make([]driver.Value, 0)
	_, err := parser.conn.Exec(sql,p)
	if err != nil {
		return false
	}
	return true
}

func (parser *eventParser) GetTableId(database string, tablename string) uint64 {
	key := database + "." + tablename
	if _, ok := parser.tableNameMap[key]; !ok {
		return uint64(0)
	}
	return parser.tableNameMap[key]
}

func (parser *eventParser) GetQueryTableName(sql string) (string, string) {
	sql = strings.Trim(sql, " ")
	if len(sql) < 11 {
		return "", ""
	}
	if strings.ToUpper(sql[0:11]) == "ALTER TABLE" {
		sqlArr := strings.Split(sql, " ")
		dbAndTable := strings.Replace(sqlArr[2], "`", "", -1)
		i := strings.IndexAny(dbAndTable, ".")
		var databaseName, tablename string
		if i > 0 {
			databaseName = dbAndTable[0:i]
			tablename = dbAndTable[i+1:]
		} else {
			databaseName = ""
			tablename = dbAndTable
		}
		return databaseName, tablename
	}
	return "", ""
}

func (mc *mysqlConn) DumpBinlog(filename string, position uint32, parser *eventParser, callbackFun callback, result chan error) (driver.Rows, error) {
	/*
	defer func() {
		if err := recover(); err != nil {
			log.Println("DumpBinlog err:",err,313)
			result <- fmt.Errorf(fmt.Sprint(err))
			return
		}
	}()
	*/
	ServerId := uint32(parser.ServerId) // Must be non-zero to avoid getting EOF packet
	flags := uint16(0)
	e := mc.writeCommandPacket(COM_BINLOG_DUMP, position, flags, ServerId, filename)
	if e != nil {
		result <- e
		return nil, e
	}

	for {
		if parser.dumpBinLogStatus != 1 {
			if parser.dumpBinLogStatus == 0 {
				time.Sleep(1 * time.Second)
				result <- fmt.Errorf("stop")
				continue
			}
			if parser.dumpBinLogStatus == 2 {
				result <- fmt.Errorf("close")
				break
			}
		}
		pkt, e := mc.readPacket()
		if e != nil {
			result <- e
			return nil, e
		} else if pkt[0] == 254 { // EOF packet
			result <- fmt.Errorf("EOF packet")
			break
		}
		if pkt[0] == 0 {
			event, _, e := parser.parseEvent(pkt[1:])
			if e != nil {
				fmt.Println("parseEvent err:",e)
				result <- e
				return nil, e
			}
			if event == nil{
				continue
			}

			//QUERY_EVENT ,must be read Schema again
			if event.Header.EventType == QUERY_EVENT {
				if SchemaName, tableName := parser.GetQueryTableName(event.Query); tableName != "" {
					if SchemaName != "" {
						event.SchemaName = SchemaName
					}
					event.TableName = tableName
					if tableId := parser.GetTableId(event.SchemaName, tableName); tableId > 0 {
						parser.GetTableSchema(tableId, event.SchemaName, tableName)
					}
				}
			}
			//only return replicateDoDb, any sql may be use db.table query
			if len(parser.replicateDoDb) > 0 {
				if _, ok := parser.replicateDoDb[event.SchemaName]; !ok {
					continue
				}
			}

			//only return EventType by set
			if parser.eventDo[int(event.Header.EventType)] == false {
				continue
			}

			if event.BinlogFileName == parser.maxBinlogFileName && event.Header.LogPos >= parser.maxBinlogPosition{
				parser.dumpBinLogStatus = 2
				break
			}
			//set binlog info
			callbackFun(event)
			parser.binlogFileName = event.BinlogFileName
			parser.binlogPosition = event.Header.LogPos

		} else {
			result <- fmt.Errorf("Unknown packet:\n%s\n\n", hex.Dump(pkt))
			if strings.Contains(string(pkt),"Could not find first log file name in binary log index file"){
				result <- fmt.Errorf("close")
				break
			}
			//result <- fmt.Errorf("Unknown packet:\n%s\n\n", hex.Dump(pkt))
		}
	}
	return nil, nil
}

type BinlogDump struct {
	DataSource string
	Status     string //stop,running,close,error,starting
	parser     *eventParser
	//BinlogIgnoreDb string
	ReplicateDoDb map[string]uint8
	OnlyEvent     []EventType
	CallbackFun   callback
	mysqlConn  MysqlConnection
	mysqlConnStatus int
	connLock sync.Mutex
}

func (This *BinlogDump) StartDumpBinlog(filename string, position uint32, ServerId uint32, result chan error,maxFileName string,maxPosition uint32) {
	This.parser = newEventParser()
	This.parser.dataSource = &This.DataSource
	This.parser.connStatus = 0
	This.parser.dumpBinLogStatus = 1
	This.parser.replicateDoDb = This.ReplicateDoDb
	This.parser.ServerId = ServerId
	This.parser.maxBinlogPosition = maxPosition
	This.parser.maxBinlogFileName = maxFileName
	for _, val := range This.OnlyEvent {
		This.parser.eventDo[int(val)] = true
	}
	log.Println(This.DataSource+ " start DumpBinlog...")
	defer func() {
		This.parser.connLock.Lock()
		if This.parser.connStatus == 1 {
			This.parser.connStatus = 0
			This.parser.conn.Close()
		}
		This.parser.connLock.Unlock()
	}()
	This.parser.binlogFileName = filename
	This.parser.binlogPosition = position
	for {
		if This.parser.dumpBinLogStatus == 3 {
			break
		}
		if This.parser.dumpBinLogStatus == 2 {
			result <- fmt.Errorf("close")
			break
		}
		result <- fmt.Errorf("starting")
		This.startConnAndDumpBinlog(result)
		time.Sleep(2 * time.Second)
	}
}

func (This *BinlogDump) checksum_enabled() {
	sql := "SHOW GLOBAL VARIABLES LIKE 'BINLOG_CHECKSUM'"
	stmt, err := This.mysqlConn.Prepare(sql)
	defer stmt.Close()
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	defer rows.Close()
	if err != nil {
		log.Println("checksum_enabled sql query err:",err)
		return
	}
	dest := make([]driver.Value, 2, 2)
	err = rows.Next(dest)
	if err != nil {
		if err.Error() != "EOF"{
			log.Println("checksum_enabled err:",err)
		}
		return
	}
	if string(dest[1].([]byte)) != ""{
		This.mysqlConn.Exec("set @master_binlog_checksum= @@global.binlog_checksum",p)
		This.parser.binlog_checksum = true
	}
	return
}

func (This *BinlogDump) startConnAndDumpBinlog(result chan error) {
	dbopen := &mysqlDriver{}
	conn, err := dbopen.Open(This.DataSource)
	if err != nil {
		result <- err
		time.Sleep(5 * time.Second)
		return
	}
	This.mysqlConn = conn.(MysqlConnection)

	//*** get connection id start
	sql := "SELECT connection_id()"
	stmt, err := This.mysqlConn.Prepare(sql)
	if err != nil{
		result <- err
		return
	}
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	var connectionId string
	for {
		dest := make([]driver.Value, 1, 1)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		connectionId = string(dest[0].([]byte))
		break
	}

	if connectionId == ""{
		return
	}
	result <- fmt.Errorf("running")
	This.parser.connectionId = connectionId
	//go This.checkDumpConnection(connectionId)
	//*** get connection id end

	This.checksum_enabled()
	This.mysqlConn.DumpBinlog(This.parser.binlogFileName, This.parser.binlogPosition, This.parser, This.CallbackFun, result)
	This.connLock.Lock()
	if This.mysqlConn != nil {
		This.mysqlConn.Close()
		This.mysqlConn = nil
	}
	This.connLock.Unlock()
	switch This.parser.dumpBinLogStatus {
	case 3:
		break
	case 2:
		result <- fmt.Errorf("close")
		This.Status = "close"
		break
	default:
		result <- fmt.Errorf("starting")
		This.Status = "stop"
	}
	This.parser.KillConnect(This.parser.connectionId)
}

func (This *BinlogDump) checkDumpConnection(connectionId string) {
	defer func() {
		if err := recover();err !=nil{
			log.Println("binlog.go checkDumpConnection err:",err)
		}
	}()
	for{
		time.Sleep(9 * time.Second)
		if This.parser.dumpBinLogStatus >= 2{
			break
		}
		var m map[string]string
		for i:=0;i<3;i++{
			m = This.parser.GetConnectionInfo(connectionId)
			if m == nil{
				time.Sleep(2 * time.Second)
				continue
			}
			break
		}
		//log.Println("GetConnectionInfo:",m)
		This.parser.connLock.Lock()
		if connectionId != This.parser.connectionId{
			This.parser.connLock.Unlock()
			break
		}
		if m == nil || m["TIME"] == ""{
			log.Println("This.mysqlConn close ,connectionId: ",connectionId)
			This.connLock.Lock()
			if This.mysqlConn != nil{
				This.mysqlConn.Close()
				This.mysqlConn = nil
			}
			This.connLock.Unlock()
			break
		}
		This.parser.connLock.Unlock()
	}
}


func (This *BinlogDump) Stop() {
	This.parser.dumpBinLogStatus = 0
}

func (This *BinlogDump) Start() {
	This.parser.dumpBinLogStatus = 1
}

func (This *BinlogDump) Close() {
	defer func() {
		if err := recover();err!=nil{
			return
		}
	}()
	This.connLock.Lock()
	defer This.connLock.Unlock()
	This.parser.dumpBinLogStatus = 2
	This.mysqlConn.Close()
	This.mysqlConn = nil
}

func (This *BinlogDump) KillDump() {
	defer func() {
		if err := recover();err!=nil{
			return
		}
	}()
	This.connLock.Lock()
	defer This.connLock.Unlock()
	This.parser.dumpBinLogStatus = 3
	This.parser.KillConnect(This.parser.connectionId)
	This.mysqlConn.Close()
	This.mysqlConn = nil
}