package src

/*
将所有数据转成 insert 的方式写入到 mysql
*/

import (
	dbDriver "database/sql/driver"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"log"
)

func (This *Conn) CommitLogMod_Append(list []*pluginDriver.PluginDataType) (errData *pluginDriver.PluginDataType) {
	//将update, delete,insert 的数据全转成  insert 语句
	var stmt dbDriver.Stmt
	n := len(list)
LOOP:
	for i := 0; i < n; i++ {
		data := list[i]
		switch data.EventType {
		case "update":
			val := make([]dbDriver.Value, 0)
			for _, v := range This.p.Field {
				var toV dbDriver.Value
				toV, This.err = This.dataTypeTransfer(This.getMySQLData(data, 1, v.FromMysqlField), v.ToField, v.ToFieldType, v.ToFieldDefault)
				if This.err != nil {
					if !This.p.BifrostMustBeSuccess {
						This.err = nil
						continue LOOP
					}
					if This.CheckDataSkip(data) {
						This.err = nil
						continue LOOP
					}
					return data
				}
				val = append(val, toV)
			}
			stmt = This.getStmt(INSERT)
			if stmt == nil {
				return data
			}
			_, This.conn.err = stmt.Exec(val)
			if This.conn.err != nil {
				if This.CheckDataSkip(data) {
					This.conn.err = nil
					continue LOOP
				}
				log.Println("plugin mysql insert exec err:", This.conn.err, " data:", val)
				return data
			}
			break
		case "insert", "delete":
			val := make([]dbDriver.Value, 0)
			for _, v := range This.p.Field {
				var toV dbDriver.Value
				toV, This.err = This.dataTypeTransfer(This.getMySQLData(data, 0, v.FromMysqlField), v.ToField, v.ToFieldType, v.ToFieldDefault)
				if This.err != nil {
					if !This.p.BifrostMustBeSuccess {
						This.err = nil
						continue LOOP
					}
					if This.CheckDataSkip(data) {
						This.err = nil
						continue LOOP
					}
					return data
				}
				val = append(val, toV)
			}
			stmt = This.getStmt(INSERT)
			if stmt == nil {
				return data
			}
			_, This.conn.err = stmt.Exec(val)
			if This.conn.err != nil {
				if This.CheckDataSkip(data) {
					This.conn.err = nil
					continue LOOP
				}
				log.Println("plugin mysql insert exec err:", This.conn.err, " data:", val)
				return data
			}
			break
		}

	}
	return
}
