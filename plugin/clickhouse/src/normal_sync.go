/*
	普通模式同步
	update 转成 insert on update
	insert 转成 replace into
	delete 转成 delete
	只要是同一条数据，只要有遍历过，后面遍历出来的数据，则不再进行操作
*/
package src

import (
	dbDriver "database/sql/driver"
	"fmt"
	driver2 "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"log"
	"strings"
)

func (This *Conn) CommitNormal(list []*pluginDriver.PluginDataType, n int) (errData *pluginDriver.PluginDataType) {
	deleteDataMap := make(map[interface{}]pluginDriver.PluginDataType, 0)
	insertDataMap := make(map[interface{}]pluginDriver.PluginDataType, 0)
	var ok bool
	var normalFun = func(v *pluginDriver.PluginDataType) {
		switch v.EventType {
		case "insert":
			for i, row := range v.Rows {
				key := This.getMySQLData(v, i, This.p.mysqlPriKey)
				if _, ok = deleteDataMap[key]; !ok {
					if _, ok = insertDataMap[key]; !ok {
						insertDataMap[key] = pluginDriver.PluginDataType{
							Timestamp:      v.Timestamp,
							EventType:      v.EventType,
							Rows:           []map[string]interface{}{row},
							Query:          v.Query,
							SchemaName:     v.SchemaName,
							TableName:      v.TableName,
							BinlogFileNum:  v.BinlogFileNum,
							BinlogPosition: v.BinlogPosition,
							Pri:            v.Pri,
						}
					}
				}
			}
			break
		case "update":
			for k := len(v.Rows) - 1; k >= 0; k-- {
				row := v.Rows[k]
				//key := row[This.p.mysqlPriKey]
				key := This.getMySQLData(v, k, This.p.mysqlPriKey)
				if k%2 == 0 {
					if _, ok := deleteDataMap[key]; !ok {
						deleteDataMap[key] = pluginDriver.PluginDataType{
							Timestamp:      v.Timestamp,
							EventType:      v.EventType,
							Rows:           []map[string]interface{}{row},
							Query:          v.Query,
							SchemaName:     v.SchemaName,
							TableName:      v.TableName,
							BinlogFileNum:  v.BinlogFileNum,
							BinlogPosition: v.BinlogPosition,
							Pri:            v.Pri,
						}
					}
				} else {
					if _, ok = deleteDataMap[key]; !ok {
						if _, ok = insertDataMap[key]; !ok {
							insertDataMap[key] = pluginDriver.PluginDataType{
								Timestamp:      v.Timestamp,
								EventType:      v.EventType,
								Rows:           []map[string]interface{}{row},
								Query:          v.Query,
								SchemaName:     v.SchemaName,
								TableName:      v.TableName,
								BinlogFileNum:  v.BinlogFileNum,
								BinlogPosition: v.BinlogPosition,
								Pri:            v.Pri,
							}
						}
					}
				}
			}
			break
		case "delete":
			if This.p.SyncType == SYNCMODE_LOG_UPDATE {
				for i, row := range v.Rows {
					key := This.getMySQLData(v, i, This.p.mysqlPriKey)
					//key := row[This.p.mysqlPriKey]
					if _, ok = deleteDataMap[key]; !ok {
						if _, ok = insertDataMap[key]; !ok {
							insertDataMap[key] = pluginDriver.PluginDataType{
								Timestamp:      v.Timestamp,
								EventType:      v.EventType,
								Rows:           []map[string]interface{}{row},
								Query:          v.Query,
								SchemaName:     v.SchemaName,
								TableName:      v.TableName,
								BinlogFileNum:  v.BinlogFileNum,
								BinlogPosition: v.BinlogPosition,
								Pri:            v.Pri,
							}
						}
					}
				}
			} else {
				for i, row := range v.Rows {
					key := This.getMySQLData(v, i, This.p.mysqlPriKey)
					//key := row[This.p.mysqlPriKey]
					if _, ok := deleteDataMap[key]; !ok {
						deleteDataMap[key] = pluginDriver.PluginDataType{
							Timestamp:      v.Timestamp,
							EventType:      v.EventType,
							Rows:           []map[string]interface{}{row},
							Query:          v.Query,
							SchemaName:     v.SchemaName,
							TableName:      v.TableName,
							BinlogFileNum:  v.BinlogFileNum,
							BinlogPosition: v.BinlogPosition,
							Pri:            v.Pri,
						}
					}
				}
			}
			break
		default:
			break
		}
	}
	for i := n - 1; i >= 0; i-- {
		v := list[i]
		normalFun(v)
	}
	var stmt driver2.Batch
	// delete 的话，将多条数据，where id in (1,2) 方式合并
	if len(deleteDataMap) > 0 {
		keys := make([]dbDriver.Value, 0)
		for key, _ := range deleteDataMap {
			keys = append(keys, key)
		}
		if len(keys) > 0 {
			var where string
			//假如字段是int的话，就 in ()
			if This.p.ckPriKeyFieldIsInt {
				where = strings.Replace(strings.Trim(fmt.Sprint(keys), "[]"), " ", ",", -1)
			} else {
				where = "'" + strings.Replace(strings.Trim(fmt.Sprint(keys), "[]"), " ", "','", -1) + "'"
			}
			sql := "ALTER TABLE " + This.p.ckDatakey + " DELETE WHERE " + This.p.ckPriKey + " in ( " + where + " )"
			if This.p.bifrostDataVersionField != "" {
				sql += " AND " + This.p.bifrostDataVersionField + " < " + fmt.Sprint(This.p.nowBifrostDataVersion)
			}
			err := This.conn.Exec(sql, []dbDriver.Value{})
			if err != nil {
				goto errLoop
			}
		}
	}

	if len(insertDataMap) > 0 {
		stmt = This.getStmt("insert")
		if stmt == nil {
			goto errLoop
		}
	LOOP:
		for _, data := range insertDataMap {
			val := make([]interface{}, 0)
			for _, v := range This.p.Field {
				var toV interface{}
				toV, This.err = CkDataTypeTransfer(This.getMySQLData(&data, 0, v.MySQL), v.CK, v.CkType, This.p.NullNotTransferDefault)
				if This.err != nil {
					if This.CheckDataSkip(&data) {
						This.err = nil
						continue LOOP
					}
					errData = &data
					goto errLoop
				}
				val = append(val, toV)
			}

			This.err = stmt.Append(val...)
			if This.err != nil {
				if This.CheckDataSkip(&data) {
					This.err = nil
					continue LOOP
				}
				errData = &data
				log.Println("plugin normal_sync.go:187 err:", This.err, " data:", val)
				stmt.Abort()
				goto errLoop
			}
		}
		if stmt != nil {
			This.err = stmt.Send()
			if This.err != nil {
				log.Println("normal_sync.go:195", This.err)
			}
		}
	}

errLoop:
	return
}
