package mysql

import (
	"time"
	"encoding/hex"
	"strings"
	"fmt"
	"runtime/debug"
	"log"
	"database/sql/driver"
)

func (mc *mysqlConn) DumpBinlog(parser *eventParser,callbackFun callback,) (driver.Rows, error) {
	defer func() {
		if err := recover(); err != nil {
			log.Println(string(debug.Stack()))
			log.Println(parser.dataSource, " binlogFileName:", parser.binlogFileName, " binlogPosition:", parser.binlogPosition)
			parser.callbackErrChan <- fmt.Errorf(fmt.Sprint(err))
			return
		}
	}()
	ServerId := uint32(parser.ServerId) // Must be non-zero to avoid getting EOF packet
	flags := uint16(0)
	e := mc.writeCommandPacket(COM_BINLOG_DUMP, parser.binlogPosition, flags, ServerId, parser.binlogFileName)
	if e != nil {
		parser.callbackErrChan <- e
		return nil, e
	}
	return mc.DumpBinlog0(parser,callbackFun)
}

func (mc *mysqlConn) DumpBinlogGtid(parser *eventParser, callbackFun callback) (driver.Rows, error) {
	defer func() {
		if err := recover(); err != nil {
			log.Println(string(debug.Stack()))
			log.Println(parser.dataSource, " binlogFileName:", parser.binlogFileName, " binlogPosition:", parser.binlogPosition)
			parser.callbackErrChan <- fmt.Errorf(fmt.Sprint(err))
			return
		}
	}()
	// 这里需要重新对 gtidSetInfo 重新做一次 ReInit 初始化
	// 在gtid事件解析后，实际 gtidSetInfo update 操作的时候 ，可能只更新指定的gtid String，并没有整体进行更新
	err := parser.gtidSetInfo.ReInit()
	if err != nil {
		return nil,err
	}
	// mysql gtid  87c74d71-2d6c-11eb-921a-0242ac110004:1-6"
	// mariadb gtid domainId-serverId-sequence
	if parser.dbType == DB_TYPE_MARIADB {
		return mc.DumpBinlogMariaDBGtid(parser,callbackFun)
	}else{
		return mc.DumpBinlogMySQLGtid(parser,callbackFun)
	}
}

func (mc *mysqlConn) DumpBinlogMySQLGtid(parser *eventParser, callbackFun callback) (driver.Rows, error) {
	ServerId := uint32(parser.ServerId) // Must be non-zero to avoid getting EOF packet
	flags := uint16(0)
	GtidBodyBytes := parser.gtidSetInfo.Encode()
	//GtidBody := GtidSet.Encode()
	e := mc.writeCommandPacket(COM_BINLOG_DUMP_GTID, GtidBodyBytes, flags, ServerId)
	if e != nil {
		parser.callbackErrChan <- e
		return nil, e
	}
	return mc.DumpBinlog0(parser,callbackFun)
}


func (mc *mysqlConn) DumpBinlogMariaDBGtid(parser *eventParser, callbackFun callback) (driver.Rows, error) {
	return nil,fmt.Errorf("mariadb gtid not supported")
}

func (mc *mysqlConn) DumpBinlog0(parser *eventParser,callbackFun callback) (driver.Rows, error) {
	for {
		parser.binlogDump.RLock()
		if parser.dumpBinLogStatus != STATUS_RUNNING {
			if parser.dumpBinLogStatus == STATUS_STOPED {
				parser.binlogDump.RUnlock()
				time.Sleep(1 * time.Second)
				parser.callbackErrChan <- fmt.Errorf(StatusFlagName(STATUS_STOPED))
				continue
			}
			if parser.dumpBinLogStatus == STATUS_CLOSED {
				parser.binlogDump.RUnlock()
				parser.callbackErrChan <- fmt.Errorf(StatusFlagName(STATUS_CLOSED))
				break
			}
		}
		parser.binlogDump.RUnlock()
		pkt, e := mc.readPacket()
		if e != nil {
			parser.callbackErrChan <- e
			return nil, e
		} else if pkt[0] == 254 { // EOF packet
			parser.callbackErrChan <- fmt.Errorf("EOF packet")
			break
			//continue
		}
		if pkt[0] == 0 {
			var event *EventReslut
			func() {
				defer func() {
					if err := recover(); err != nil {
						e = fmt.Errorf("parseEvent err recover err:%s ;lastMapEvent:%T ;binlogFileName:%s ;binlogPosition:%d",fmt.Sprint(err),parser.lastMapEvent,parser.binlogFileName,parser.binlogPosition)
						log.Println(string(debug.Stack()))
					}
				}()
				event, _, e = parser.parseEvent(pkt[1:])
			}()
			if e != nil {
				//假如解析异常 ,就直接close掉
				e = fmt.Errorf("parseEvent err:" + e.Error())
				fmt.Println(e)
				parser.callbackErrChan <- e
				return nil, e
			}
			if event == nil {
				continue
			}
			if parser.maxBinlogFileName != "" {
				if event.BinlogFileName == parser.maxBinlogFileName && event.Header.LogPos >= parser.maxBinlogPosition {
					parser.binlogDump.Lock()
					parser.dumpBinLogStatus = STATUS_CLOSED
					parser.binlogDump.Unlock()
					break
				}
			}
			event.EventID = parser.getNextEventID()
			switch event.Header.EventType {
			//这里要判断一下如果是row事件
			//在map event的时候已经判断过了是否要过滤，所以判断一下 parser.filterNextRowEvent 是否为true
			case WRITE_ROWS_EVENTv0, WRITE_ROWS_EVENTv1, WRITE_ROWS_EVENTv2, UPDATE_ROWS_EVENTv0, UPDATE_ROWS_EVENTv1, UPDATE_ROWS_EVENTv2, DELETE_ROWS_EVENTv0, DELETE_ROWS_EVENTv1, DELETE_ROWS_EVENTv2:
				if parser.filterNextRowEvent == true {
					continue
				}
				break
			case QUERY_EVENT:
				if event.Query == "COMMIT" {
					break
				}
				// # Dumm
				// # Dummy e
				// # Dum
				// # Dummy event replacing event type 16
				// mariadb Dumm 内容事件,这种内容的事件，直接过滤掉，不展示给上层
				if event.Query[0:1] == "#"{
					continue
				}

				//only return replicateDoDb, any sql may be use db.table query
				var SchemaName,tableName string
				var isRename bool
				if SchemaName, tableName,isRename = parser.GetQueryTableName(event.Query); tableName != "" {
					if SchemaName != "" {
						event.SchemaName = SchemaName
					}
					event.TableName = tableName
				}
				if event.TableName != "" {
					if parser.binlogDump.CheckReplicateDb(event.SchemaName, event.TableName) == false {
						parser.saveBinlog(event)
						continue
					}
					if isRename {
						// 假如 是rename 操作的 ddl,需要将 SchemaName,TableName 对应的缓存数据删除，因为表名变了，TableId 也变了
						parser.delTableId(event.SchemaName, event.TableName)
					}else{
						if tableId, err := parser.GetTableId(event.SchemaName, event.TableName); err == nil {
							parser.GetTableSchema(tableId, event.SchemaName, event.TableName)
						}
					}
					break
				}
				// 假如 drop database schemaName 这样的语句，只有 SchemaName，而没有 TableName的，则匹配是否要过滤整个库
				if event.SchemaName != "" {
					if parser.binlogDump.CheckReplicateDb(event.SchemaName, "*") == false {
						parser.saveBinlog(event)
						continue
					}
				}
				break
			case XID_EVENT:
				parser.saveBinlog(event)
				break
			default:
				if event.TableName != "" && parser.binlogDump.CheckReplicateDb(event.SchemaName, event.TableName) == false {
					parser.saveBinlog(event)
					continue
				}
			}

			//only return EventType by set
			if parser.eventDo[int(event.Header.EventType)] == false {
				parser.saveBinlog(event)
				continue
			}
			callbackFun(event)
			parser.saveBinlog(event)

		} else {
			parser.callbackErrChan <- fmt.Errorf("Unknown packet:\n%s\n\n", hex.Dump(pkt))
			if strings.Contains(string(pkt), "Could not find first log file name in binary log index file") {
				parser.callbackErrChan <- fmt.Errorf(StatusFlagName(STATUS_CLOSED))
				break
			}
			//result <- fmt.Errorf("Unknown packet:\n%s\n\n", hex.Dump(pkt))
		}
	}
	return nil, nil
}
