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
	"runtime/debug"
)

type eventParser struct {
	sync.RWMutex
	format           *FormatDescriptionEvent
	tableMap         map[uint64]*TableMapEvent
	tableNameMap     map[string]uint64
	tableSchemaMap   map[uint64]*tableStruct
	dataSource       *string
	connStatus       int8 //0 stop  1 running
	conn             MysqlConnection
	dumpBinLogStatus uint8 //0 stop ,1 running
	binlogFileName   string
	binlogPosition   uint32
	binlogTimestamp  uint32
	maxBinlogFileName   string
	maxBinlogPosition   uint32
	eventDo          []bool
	ServerId         uint32
	connectionId	 string
	connLock 		 sync.Mutex
	binlog_checksum  bool
	filterNextRowEvent bool
	binlogDump		*BinlogDump
}

func newEventParser(binlogDump *BinlogDump) (parser *eventParser) {
	parser = new(eventParser)
	parser.tableMap = make(map[uint64]*TableMapEvent)
	parser.tableNameMap = make(map[string]uint64)
	parser.tableSchemaMap = make(map[uint64]*tableStruct)
	parser.eventDo = make([]bool, 36, 36)
	parser.ServerId = 1
	parser.connectionId = ""
	parser.maxBinlogFileName = ""
	parser.maxBinlogPosition = 0
	parser.binlog_checksum = false
	parser.filterNextRowEvent = false
	parser.binlogDump = binlogDump
	return
}

func (parser *eventParser) saveBinlog(event *EventReslut){
	switch event.Header.EventType {
	case QUERY_EVENT:
		parser.binlogFileName = event.BinlogFileName
		parser.binlogPosition = event.Header.LogPos
		parser.binlogTimestamp = event.Header.Timestamp
		break
	default:
		break
	}
}

func (parser *eventParser) parseEvent(data []byte) (event *EventReslut, filename string, err error) {
	var buf *bytes.Buffer
	if parser.binlog_checksum {
		buf = bytes.NewBuffer(data[0:len(data)-4])
	}else{
		buf = bytes.NewBuffer(data)
	}

	filename = parser.binlogFileName
	//log.Println("EventType:",EventType(data[4]))
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
		break
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
		parser.tableSchemaMap = make(map[uint64]*tableStruct,0)
		break
	case TABLE_MAP_EVENT:
		var table_map_event *TableMapEvent
		table_map_event, err = parser.parseTableMapEvent(buf)
		//log.Println("table_map_event:",table_map_event)
		parser.tableMap[table_map_event.tableId] = table_map_event
		//log.Println("table_map_event:",*table_map_event,"tableId:",table_map_event.tableId," schemaName:",table_map_event.schemaName," tableName:",table_map_event.tableName)
		if parser.binlogDump.CheckReplicateDb(table_map_event.schemaName,table_map_event.tableName) == false{
			parser.filterNextRowEvent = true
		}else{
			parser.filterNextRowEvent = false
			if _, ok := parser.tableSchemaMap[table_map_event.tableId]; !ok {
				parser.GetTableSchema(table_map_event.tableId, table_map_event.schemaName, table_map_event.tableName)
			}
		}
		/*
		if _, ok := parser.tableSchemaMap[table_map_event.tableId]; !ok {
			parser.GetTableSchema(table_map_event.tableId, table_map_event.schemaName, table_map_event.tableName)
		}
		*/
		event = &EventReslut{
			Header:         table_map_event.header,
			BinlogFileName: parser.binlogFileName,
			BinlogPosition: parser.binlogPosition,
			SchemaName:     parser.tableMap[table_map_event.tableId].schemaName,
			TableName:      parser.tableMap[table_map_event.tableId].tableName,
		}
		break
	case WRITE_ROWS_EVENTv0,WRITE_ROWS_EVENTv1,WRITE_ROWS_EVENTv2, UPDATE_ROWS_EVENTv0,UPDATE_ROWS_EVENTv1,UPDATE_ROWS_EVENTv2, DELETE_ROWS_EVENTv0,DELETE_ROWS_EVENTv1,DELETE_ROWS_EVENTv2:
		var rowsEvent *RowsEvent
		rowsEvent, err = parser.parseRowsEvent(buf)
		if err != nil{
			log.Println("row event err:",err)
		}
		if _,ok:=parser.tableSchemaMap[rowsEvent.tableId];ok {
			event = &EventReslut{
				Header:         rowsEvent.header,
				BinlogFileName: parser.binlogFileName,
				BinlogPosition: parser.binlogPosition,
				SchemaName:     parser.tableMap[rowsEvent.tableId].schemaName,
				TableName:      parser.tableMap[rowsEvent.tableId].tableName,
				Rows:           rowsEvent.rows,
				Pri:            parser.tableSchemaMap[rowsEvent.tableId].Pri,
			}
		}else{
			event = &EventReslut{
				Header:         rowsEvent.header,
				BinlogFileName: parser.binlogFileName,
				BinlogPosition: parser.binlogPosition,
				SchemaName:     parser.tableMap[rowsEvent.tableId].schemaName,
				TableName:      parser.tableMap[rowsEvent.tableId].tableName,
				Rows:           rowsEvent.rows,
				Pri:            make([]*string,0),
			}
		}
		break
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
	//var errPrint bool = false
	var lastErr string
	for {
		parser.connLock.Lock()
		err := parser.GetTableSchemaByName(tableId,database,tablename)
		parser.connLock.Unlock()
		if err == nil{
			break
		}else{
			if lastErr != err.Error() {
				log.Println("binlog GetTableSchema err:", err, " tableId:", tableId, " database:", database, " tablename:", tablename)
				lastErr = err.Error()
			}
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
			errs = fmt.Errorf(string(debug.Stack()))
		}
	}()
	if parser.connStatus == 0 {
		parser.initConn()
	}
	//set dbAndTable Name tableId
	parser.tableNameMap[database+"."+tablename] = tableId
	sql := "SELECT COLUMN_NAME,COLUMN_KEY,COLUMN_TYPE,CHARACTER_SET_NAME,COLLATION_NAME,NUMERIC_SCALE,EXTRA,COLUMN_DEFAULT,DATA_TYPE,CHARACTER_OCTET_LENGTH FROM information_schema.columns WHERE table_schema='" + database + "' AND table_name='" + tablename + "' ORDER BY `ORDINAL_POSITION` ASC"
	stmt, err := parser.conn.Prepare(sql)
	if err != nil{
		errs = err
		return
	}
	defer stmt.Close()
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	if err != nil {
		errs = err
		return
	}
	defer rows.Close()
	//columeArr := make([]*tableStructcolumn_schema_type,0)
	tableInfo := &tableStruct{
		Pri:					make([]*string,0),
		ColumnSchemaTypeList:	make([]*column_schema_type,0),
	}
	for {
		dest := make([]driver.Value, 10, 10)
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
		var COLUMN_DEFAULT	string
		var DATA_TYPE string
		var CHARACTER_OCTET_LENGTH uint64

		COLUMN_NAME = dest[0].(string)
		COLUMN_KEY = dest[1].(string)
		COLUMN_TYPE = dest[2].(string)
		if dest[3] == nil{
			CHARACTER_SET_NAME = ""
		}else{
			CHARACTER_SET_NAME = dest[3].(string)
		}

		if dest[4] == nil{
			COLLATION_NAME = ""
		}else{
			COLLATION_NAME = dest[4].(string)
		}

		if dest[5] == nil{
			NUMERIC_SCALE = ""
		}else{
			NUMERIC_SCALE = fmt.Sprint(dest[5])
		}

		EXTRA = dest[6].(string)

		DATA_TYPE = dest[8].(string)

		//bit类型这个地方比较特殊，不能直接转成string，并且当前只有 time,datetime 类型转换的时候会用到 默认值，这里不进行其他细节处理
		if DATA_TYPE != "bit"{
			if dest[7] == nil{
				COLUMN_DEFAULT = ""
			}else{
				COLUMN_DEFAULT = dest[7].(string)
			}
		}

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

		if dest[9] == nil{
			CHARACTER_OCTET_LENGTH = 0
		}else{
			switch dest[9].(type) {
			case uint32:
				CHARACTER_OCTET_LENGTH = uint64(dest[9].(uint32))
			case uint64:
				CHARACTER_OCTET_LENGTH = dest[9].(uint64)
			default:
				CHARACTER_OCTET_LENGTH = 0
			}
		}

		tableInfo.ColumnSchemaTypeList = append(tableInfo.ColumnSchemaTypeList, &column_schema_type{
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
			COLUMN_DEFAULT:COLUMN_DEFAULT,
			DATA_TYPE:DATA_TYPE,
			CHARACTER_OCTET_LENGTH:CHARACTER_OCTET_LENGTH,
		})

		if strings.ToUpper(COLUMN_KEY) == "PRI"{
			tableInfo.Pri = append(tableInfo.Pri,&COLUMN_NAME)
		}
	}
	if len(tableInfo.ColumnSchemaTypeList) == 0{
		return fmt.Errorf("column len is 0 "+"db:"+database+" table:"+tablename+" tableId:"+fmt.Sprint(tableId)+" may be no privilege")
	}
	parser.tableSchemaMap[tableId] = tableInfo
	errs = nil
	return
}

func (parser *eventParser) GetConnectionInfo(connectionId string) (m map[string]string,e error){
	parser.connLock.Lock()
	defer func() {
		if err := recover(); err != nil {
			if parser.connStatus == 1{
				parser.connStatus = 0
				parser.conn.Close()
			}
			log.Println("binlog.go GetConnectionInfo err:",err)
			m = nil
		}
		parser.connLock.Unlock()
	}()
	if parser.connStatus == 0 {
		parser.initConn()
	}
	sql := "select TIME,STATE from `information_schema`.`PROCESSLIST` WHERE ID='"+connectionId+"'"
	stmt, err := parser.conn.Prepare(sql)
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	if err != nil {
		return nil,err
	}
	m = make(map[string]string,2)
	for {
		dest := make([]driver.Value, 2, 2)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		m["TIME"] = fmt.Sprint(dest[0])
		m["STATE"]= dest[1].(string)
		break
	}
	return m,nil
}


func (parser *eventParser) KillConnect(connectionId string) (b bool){
	if connectionId == ""{
		return true
	}
	b = false
	parser.connLock.Lock()
	defer func() {
		if err := recover(); err != nil {
			if parser.connStatus == 1{
				parser.connStatus = 0
				parser.conn.Close()
			}
			b = false
		}
		parser.connLock.Unlock()
	}()
	if parser.connStatus == 0 {
		parser.initConn()
	}
	sql := "kill "+connectionId
	_, err := parser.conn.Exec(sql,[]driver.Value{})
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
	defer func() {
		if err := recover(); err != nil {
			log.Println(string(debug.Stack()))
			log.Println(parser.dataSource," binlogFileName:",parser.binlogFileName," binlogPosition:",parser.binlogPosition)
			result <- fmt.Errorf(fmt.Sprint(err))

			return
		}
	}()
	parser.binlogFileName = filename
	parser.binlogPosition = position
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
			//continue
		}
		if pkt[0] == 0 {
			var event *EventReslut
			func(){
				defer func() {
					if err := recover(); err != nil {
						e = fmt.Errorf(fmt.Sprint(err))
						log.Println(string(debug.Stack()))
					}
				}()
				event, _, e = parser.parseEvent(pkt[1:])
			}()
			if e != nil {
				//假如解析异常 ,就直接close掉
				e = fmt.Errorf("parseEvent err:"+ e.Error())
				fmt.Println(e)
				result <- e
				return nil,e
			}
			if event == nil{
				continue
			}
			if parser.maxBinlogFileName != "" {
				if event.BinlogFileName == parser.maxBinlogFileName && event.Header.LogPos >= parser.maxBinlogPosition {
					parser.dumpBinLogStatus = 2
					break
				}
			}

			//QUERY_EVENT ,must be read Schema again
			if event.Header.EventType == QUERY_EVENT {
				if SchemaName, tableName := parser.GetQueryTableName(event.Query); tableName != "" {
					if SchemaName != "" {
						event.SchemaName = SchemaName
					}
					event.TableName = tableName
					if parser.binlogDump.CheckReplicateDb(event.SchemaName,tableName) == false{
						parser.saveBinlog(event)
						continue
					}
					if tableId := parser.GetTableId(event.SchemaName, tableName); tableId > 0 {
						parser.GetTableSchema(tableId, event.SchemaName, tableName)
					}
				}
			}
			//only return replicateDoDb, any sql may be use db.table query

			//这里要判断一下如果是row事件
			//在map event的时候已经判断过了是否要过滤，所以判断一下 parser.filterNextRowEvent 是否为true
			switch event.Header.EventType {
			case WRITE_ROWS_EVENTv0, WRITE_ROWS_EVENTv1, WRITE_ROWS_EVENTv2, UPDATE_ROWS_EVENTv0, UPDATE_ROWS_EVENTv1, UPDATE_ROWS_EVENTv2, DELETE_ROWS_EVENTv0, DELETE_ROWS_EVENTv1, DELETE_ROWS_EVENTv2:
				if parser.filterNextRowEvent == true {
					continue
				}
				break
			default:
				if parser.binlogDump.CheckReplicateDb(event.SchemaName,event.TableName) == false{
					parser.saveBinlog(event)
					continue
				}
				break
			}

			//only return EventType by set
			if parser.eventDo[int(event.Header.EventType)] == false {
				parser.saveBinlog(event)
				continue
			}

			/*
			log.Println("event:",event)
			log.Println("event BinlogFileName:",event.BinlogFileName)
			log.Println("event BinlogPosition:",event.BinlogPosition)
			log.Println("event Query:",event.Query)
			log.Println("event Rows:",event.Rows)
			log.Println("event SchemaName:",event.SchemaName)
			log.Println("event TableName:",event.TableName)
			log.Println("event EventType:",event.Header.EventType)
			*/
			//set binlog info
			callbackFun(event)
			parser.saveBinlog(event)

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
	sync.RWMutex
	connLock 			sync.Mutex
	DataSource 			string
	Status     			BinlogDumpStatus //stop,running,close,error,starting
	parser     			*eventParser
	ReplicateDoDb 		map[string]map[string]uint8
	ReplicateDoDbLike 	map[string]map[string]uint8
	replicateDoDbCheck  bool
	ReplicateIgnoreDb 	map[string]map[string]uint8
	ReplicateIgnoreDbLike map[string]map[string]uint8
	replicateIgnoreDbCheck bool
	OnlyEvent     		[]EventType
	CallbackFun   		callback
	mysqlConn  			MysqlConnection
	mysqlConnStatus 	int
	checkSlaveStatus    bool
}


func NewBinlogDump(DataSource string,CallbackFun callback, OnlyEvent []EventType,ReplicateDoDb,ReplicateIgnoreDb map[string]map[string]uint8) *BinlogDump{
	var replicateDoDbCheck,replicateIgnoreDbCheck bool = false,false
	if ReplicateDoDb != nil{
		replicateDoDbCheck = true
	}
	if ReplicateIgnoreDb != nil{
		replicateIgnoreDbCheck = true
	}

	return &BinlogDump{
		DataSource:DataSource,
		Status:BINLOG_DUMP_CLOSE,
		ReplicateDoDb:ReplicateDoDb,
		ReplicateIgnoreDb:ReplicateIgnoreDb,
		OnlyEvent:OnlyEvent,
		CallbackFun:CallbackFun,
		checkSlaveStatus:false,
		replicateDoDbCheck:replicateDoDbCheck,
		replicateIgnoreDbCheck:replicateIgnoreDbCheck,
	}
}

func (This *BinlogDump) GetBinlog()(string,uint32,uint32){
	return This.parser.binlogFileName,This.parser.binlogPosition,This.parser.binlogTimestamp
}


func (This *BinlogDump) StartDumpBinlog(filename string, position uint32, ServerId uint32, result chan error,maxFileName string,maxPosition uint32) {
	This.parser = newEventParser(This)
	This.parser.dataSource = &This.DataSource
	This.parser.connStatus = 0
	This.parser.dumpBinLogStatus = 1
	This.parser.ServerId = ServerId
	This.parser.maxBinlogPosition = maxPosition
	This.parser.maxBinlogFileName = maxFileName
	for _, val := range This.OnlyEvent {
		This.parser.eventDo[int(val)] = true
	}
	log.Println(This.DataSource+ " start DumpBinlog... filename:",filename, " position:",position)
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
	first := true
	for {
		if This.parser.dumpBinLogStatus == 3 {
			break
		}
		if This.parser.dumpBinLogStatus == 2 {
			result <- fmt.Errorf("close")
			break
		}
		if first == false{
			time.Sleep(5 * time.Second)
		}
		first = false
		result <- fmt.Errorf("starting")
		This.startConnAndDumpBinlog(result)
	}
}

func (This *BinlogDump) checksum_enabled() {
	sql := "SHOW GLOBAL VARIABLES LIKE 'BINLOG_CHECKSUM'"
	stmt, err := This.mysqlConn.Prepare(sql)
	if err != nil{
		return
	}
	defer stmt.Close()
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	if err != nil {
		log.Println("checksum_enabled sql query err:",err)
		return
	}
	defer rows.Close()
	dest := make([]driver.Value, 2, 2)
	err = rows.Next(dest)
	if err != nil {
		if err.Error() != "EOF"{
			log.Println("checksum_enabled err:",err)
		}
		return
	}
	if dest[1].(string) != ""{
		This.mysqlConn.Exec("set @master_binlog_checksum= @@global.binlog_checksum",p)
		This.parser.binlog_checksum = true
	}
	log.Println("binlog_checksum:",This.parser.binlog_checksum)
	return
}

func (This *BinlogDump) startConnAndDumpBinlog(result chan error) {
	defer func() {
		if err := recover();err!=nil{
			log.Println("startConnAndDumpBinlog err:",err)
			result <- fmt.Errorf(fmt.Sprint(err))
			log.Println(string(debug.Stack()))
		}
	}()
	dbopen := &mysqlDriver{}
	conn, err := dbopen.Open(This.DataSource)
	if err != nil {
		result <- err
		//time.Sleep(5 * time.Second)
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
	if err != nil {
		return
	}
	var connectionId string
	for {
		dest := make([]driver.Value, 1, 1)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		connectionId = fmt.Sprint(dest[0])
		break
	}
	rows.Close()
	stmt.Close()

	if connectionId == ""{
		return
	}
	result <- fmt.Errorf("running")
	This.parser.connectionId = connectionId
	go This.checkDumpConnection()
	//*** get connection id end

	This.checksum_enabled()
	This.mysqlConn.DumpBinlog(This.parser.binlogFileName, This.parser.binlogPosition, This.parser, This.CallbackFun, result)
	This.connLock.Lock()
	if This.mysqlConn != nil {
		func(){
			defer func() {
				if err:=recover();err != nil{
					return
				}
			}()
			This.mysqlConn.Close()
		}()
		This.mysqlConn = nil
	}
	This.connLock.Unlock()
	switch This.parser.dumpBinLogStatus {
	case 3:
		break
	case 2:
		result <- fmt.Errorf("close")
		This.Status = BINLOG_DUMP_CLOSE
		break
	default:
		result <- fmt.Errorf("starting")
		This.Status = BINLOG_DUMP_STOP
	}
	This.parser.KillConnect(connectionId)
}

func (This *BinlogDump) checkDumpConnection() {
	defer func() {
		if err := recover();err !=nil{
			log.Println("binlog.go checkDumpConnection err:",err)
		}
	}()
	This.parser.connLock.Lock()
	if This.checkSlaveStatus == true{
		This.parser.connLock.Unlock()
		return
	}
	This.checkSlaveStatus = true
	This.parser.connLock.Unlock()
	defer func() {
		This.checkSlaveStatus = false
	}()
	for{
		time.Sleep(9 * time.Second)
		if This.parser.dumpBinLogStatus >= 2{
			break
		}
		This.parser.connLock.Lock()
		connectionId := This.parser.connectionId
		This.parser.connLock.Unlock()

		var m map[string]string
		var e error

		for i:=0;i<3;i++{
			m,e = This.parser.GetConnectionInfo(connectionId)
			if e != nil{
				time.Sleep(2 * time.Second)
				continue
			}
			break
		}

		//log.Println("GetConnectionInfo:",m)
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
	if This.mysqlConn!=nil{
		This.mysqlConn.Close()
	}
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
	if This.mysqlConn!=nil{
		This.mysqlConn.Close()
	}
}