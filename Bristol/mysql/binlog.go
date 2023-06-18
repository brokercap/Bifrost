package mysql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"log"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

type BinlogDump struct {
	sync.RWMutex
	DataSource             string
	Status                 StatusFlag //stop,running,close,error,starting
	parser                 *eventParser
	ReplicateDoDb          map[string]map[string]uint8
	ReplicateDoDbLike      map[string]map[string]uint8
	replicateDoDbCheck     bool
	ReplicateIgnoreDb      map[string]map[string]uint8
	ReplicateIgnoreDbLike  map[string]map[string]uint8
	replicateIgnoreDbCheck bool
	OnlyEvent              []EventType
	CallbackFun            callback
	mysqlConn              MysqlConnection
	mysqlConnStatus        int
	checkSlaveStatus       bool
	context                struct {
		ctx        context.Context
		cancelFunc context.CancelFunc
	}
}

func NewBinlogDump(DataSource string, CallbackFun callback, OnlyEvent []EventType, ReplicateDoDb, ReplicateIgnoreDb map[string]map[string]uint8) *BinlogDump {
	var replicateDoDbCheck, replicateIgnoreDbCheck bool = false, false
	if ReplicateDoDb != nil {
		replicateDoDbCheck = true
	}
	if ReplicateIgnoreDb != nil {
		replicateIgnoreDbCheck = true
	}

	binlogDump := &BinlogDump{
		DataSource:             DataSource,
		Status:                 STATUS_CLOSED,
		ReplicateDoDb:          ReplicateDoDb,
		ReplicateIgnoreDb:      ReplicateIgnoreDb,
		OnlyEvent:              OnlyEvent,
		CallbackFun:            CallbackFun,
		checkSlaveStatus:       false,
		replicateDoDbCheck:     replicateDoDbCheck,
		replicateIgnoreDbCheck: replicateIgnoreDbCheck,
	}
	binlogDump.parser = newEventParser(binlogDump)
	return binlogDump
}

func (This *BinlogDump) SetNextEventID(id uint64) bool {
	This.Lock()
	defer This.Unlock()
	if This.parser.nextEventID <= 0 {
		This.parser.nextEventID = id
		return true
	}
	return false
}

func (This *BinlogDump) GetBinlog() (string, uint32, uint32, string, uint64) {
	This.RLock()
	defer This.RUnlock()
	return This.parser.binlogFileName, This.parser.binlogPosition, This.parser.binlogTimestamp, This.parser.getGtid(), This.parser.lastEventID
}

func (This *BinlogDump) StartDumpBinlog(filename string, position uint32, ServerId uint32, result chan error, maxFileName string, maxPosition uint32) {
	if This.parser == nil {
		This.parser = newEventParser(This)
	}
	This.parser.ServerId = ServerId
	This.parser.maxBinlogFileName = maxFileName
	This.parser.maxBinlogPosition = maxPosition
	This.parser.binlogFileName = filename
	This.parser.binlogPosition = position
	This.parser.callbackErrChan = result
	This.StartDumpBinlog0()
}

func (This *BinlogDump) StartDumpBinlogGtid(gtid string, ServerId uint32, result chan error) {
	if This.parser == nil {
		This.parser = newEventParser(This)
	}
	gtidInfo, dbType, err := NewGTIDSet(gtid)
	if err != nil {
		result <- err
		return
	}
	This.parser.dbType = dbType
	This.parser.gtidSetInfo = gtidInfo
	This.parser.ServerId = ServerId
	This.parser.callbackErrChan = result
	This.parser.isGTID = true
	This.StartDumpBinlog0()
}

func (This *BinlogDump) StartDumpBinlog0() {
	This.context.ctx, This.context.cancelFunc = context.WithCancel(context.Background())
	This.parser.dataSource = &This.DataSource
	This.parser.connStatus = STATUS_CLOSED
	This.parser.dumpBinLogStatus = STATUS_RUNNING
	for _, val := range This.OnlyEvent {
		This.parser.eventDo[int(val)] = true
	}
	log.Println(This.DataSource+" start DumpBinlog... gtid:", This.parser.getGtid(), " binlogFileName:", This.parser.binlogFileName, " binlogPosition:", This.parser.binlogPosition)
	defer func() {
		This.parser.ParserConnClose(true)
	}()

	var first = true
	for {
		This.RLock()
		if This.parser.dumpBinLogStatus == STATUS_KILLED {
			This.RUnlock()
			break
		}
		if This.parser.dumpBinLogStatus == STATUS_CLOSED {
			This.RUnlock()
			This.parser.callbackErrChan <- fmt.Errorf(StatusFlagName(STATUS_CLOSED))
			break
		}
		This.RUnlock()
		if first == false {
			time.Sleep(5 * time.Second)
		} else {
			first = false
		}
		This.parser.callbackErrChan <- fmt.Errorf(StatusFlagName(STATUS_STARTING))
		This.startConnAndDumpBinlog()
	}
}

// 这个函数里使用 conn 不用加锁
// 因为这个函数只有在 binlog dump 连接初始化的时候，会被执行
func (This *BinlogDump) checksumEnabled() {
	sql := "SHOW GLOBAL VARIABLES LIKE 'BINLOG_CHECKSUM'"
	stmt, err := This.mysqlConn.Prepare(sql)
	if err != nil {
		return
	}
	defer stmt.Close()
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	if err != nil {
		log.Println("checksum_enabled sql query err:", err)
		return
	}
	defer rows.Close()
	dest := make([]driver.Value, 2, 2)
	err = rows.Next(dest)
	if err != nil {
		if err.Error() != "EOF" {
			log.Println("checksum_enabled err:", err)
		}
		return
	}
	if dest[1].(string) != "" && strings.ToLower(dest[1].(string)) != "none" {
		This.mysqlConn.Exec("set @master_binlog_checksum= @@global.binlog_checksum", p)
		This.parser.binlog_checksum = true
	}
	log.Println("binlog_checksum:", This.parser.binlog_checksum)
	return
}

func (This *BinlogDump) BinlogConnCLose(lock bool) {
	if lock == true {
		This.Lock()
		defer This.Unlock()
	}
	if This.mysqlConn != nil {
		func() {
			defer func() {
				if err := recover(); err != nil {
					return
				}
			}()
			This.mysqlConn.Close()
		}()
		This.mysqlConn = nil
	}
}

// 只用于发起 close 信号，不管其他的
func (This *BinlogDump) BinlogConnCLose0(lock bool) {
	if lock == true {
		This.Lock()
		defer This.Unlock()
	}
	defer func() {
		if err := recover(); err != nil {
			return
		}
	}()
	if This.mysqlConn != nil {
		This.mysqlConn.Close()
	}
}

func (This *BinlogDump) startConnAndDumpBinlog() {
	defer func() {
		if err := recover(); err != nil {
			log.Println("startConnAndDumpBinlog err:", err)
			This.parser.callbackErrChan <- fmt.Errorf(fmt.Sprint(err))
			log.Println(string(debug.Stack()))
		}
	}()
	dbopen := &mysqlDriver{}
	conn, err := dbopen.Open(This.DataSource)
	if err != nil {
		This.parser.callbackErrChan <- err
		//time.Sleep(5 * time.Second)
		return
	}
	This.mysqlConn = conn.(MysqlConnection)

	//*** get connection id start
	sql := "SELECT connection_id()"
	stmt, err := This.mysqlConn.Prepare(sql)
	if err != nil {
		This.parser.callbackErrChan <- err
		return
	}
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	if err != nil {
		stmt.Close()
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

	if connectionId == "" {
		return
	}
	This.parser.callbackErrChan <- fmt.Errorf(StatusFlagName(STATUS_RUNNING))
	This.parser.connectionId = connectionId
	ctx, cancelFun := context.WithCancel(This.context.ctx)
	go This.checkDumpConnection(ctx, connectionId)
	//*** get connection id end

	This.checksumEnabled()
	if This.parser.isGTID == false {
		This.mysqlConn.DumpBinlog(This.parser, This.CallbackFun)
	} else {
		This.mysqlConn.DumpBinlogGtid(This.parser, This.CallbackFun)
	}
	This.BinlogConnCLose(true)
	This.RLock()
	This.Status = This.parser.dumpBinLogStatus
	// 通知上一层状态变更
	switch This.parser.dumpBinLogStatus {
	case STATUS_KILLED:
		break
	default:
		This.parser.callbackErrChan <- fmt.Errorf(StatusFlagName(This.parser.dumpBinLogStatus))
		break
	}
	This.RUnlock()
	cancelFun()
	This.parser.KillConnect(connectionId)
}

func (This *BinlogDump) checkDumpConnection(ctx context.Context, connectionId string) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("binlog.go checkDumpConnection err:", err)
		}
	}()
	This.Lock()
	if This.checkSlaveStatus == true {
		This.Unlock()
		return
	}
	This.checkSlaveStatus = true
	This.Unlock()
	defer func() {
		This.Lock()
		This.checkSlaveStatus = false
		This.Unlock()
	}()
	var ok bool
	timeout := 9 * time.Second
	timer := time.NewTimer(timeout)

	for {
		timer.Reset(timeout)
		select {
		case <-timer.C:
			timer.Stop()
			This.Lock()
			if This.parser.dumpBinLogStatus == STATUS_CLOSED || This.parser.dumpBinLogStatus == STATUS_KILLED {
				This.Unlock()
				break
			}
			connectionId := This.parser.connectionId
			This.Unlock()

			var m map[string]string
			var e error

			for i := 0; i < 3; i++ {
				m, e = This.parser.GetConnectionInfo(connectionId)
				if e != nil {
					time.Sleep(2 * time.Second)
					continue
				}
				break
			}
			if m != nil {
				if _, ok = m["TIME"]; !ok {
					log.Println("This.mysqlConn close ,connectionId: ", connectionId)
					This.BinlogConnCLose0(true)
					return
				}
			}
		case <-ctx.Done():
			return
		}

	}
}

func (This *BinlogDump) UpdateUri(connUri string) {
	This.Lock()
	This.DataSource = connUri
	This.Unlock()
}

func (This *BinlogDump) Stop() {
	This.Lock()
	This.parser.dumpBinLogStatus = STATUS_STOPED
	This.Unlock()
}

func (This *BinlogDump) Start() {
	This.Lock()
	This.parser.dumpBinLogStatus = STATUS_RUNNING
	This.Unlock()
}

func (This *BinlogDump) Close() {
	defer func() {
		if err := recover(); err != nil {
			return
		}
	}()
	This.Lock()
	defer This.Unlock()
	This.parser.dumpBinLogStatus = STATUS_CLOSED
	if This.context.cancelFunc != nil {
		This.context.cancelFunc()
		This.context.cancelFunc = nil
	}
	This.BinlogConnCLose(false)
}

func (This *BinlogDump) KillDump() {
	defer func() {
		if err := recover(); err != nil {
			log.Println("KillDump err:", err)
			log.Println(string(debug.Stack()))
		}
	}()
	var connectId string
	This.Lock()
	if This.parser != nil {
		This.parser.dumpBinLogStatus = STATUS_KILLED
		connectId = This.parser.connectionId
	}
	This.Unlock()
	if connectId != "" {
		This.parser.KillConnect(connectId)
	}
	This.BinlogConnCLose(true)
}
