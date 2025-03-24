package src

/*
将所有数据转成 insert 的方式写入到 mysql
*/

import (
	dbDriver "database/sql/driver"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"log"
)

func (This *Conn) CommitLogMod_Append(list []*pluginDriver.PluginDataType, n int) (errData *pluginDriver.PluginDataType) {
	var stmt dbDriver.Stmt
LOOP:
	for i := 0; i < n; i++ {
		vData := list[i]
		val := make([]dbDriver.Value, 0)
		l := len(vData.Rows)
		switch vData.EventType {
		case "insert", "delete":
			for k := 0; k < l; k++ {
				for _, v := range This.p.Field {
					var toV interface{}
					toV, This.err = CkDataTypeTransfer(This.getMySQLData(vData, k, v.MySQL), v.CK, v.CkType, This.p.NullNotTransferDefault)
					if This.err != nil {
						if This.CheckDataSkip(vData) {
							This.err = nil
							continue LOOP
						}
						errData = vData
						goto errLoop
					}
					val = append(val, toV)
				}
			}
			break
		case "update":
			for k := 0; k < l; k++ {
				// 取奇数下标，则为更新的具体值
				if k&1 == 1 {
					for _, v := range This.p.Field {
						var toV interface{}
						toV, This.err = CkDataTypeTransfer(This.getMySQLData(vData, k, v.MySQL), v.CK, v.CkType, This.p.NullNotTransferDefault)
						if This.err != nil {
							if This.CheckDataSkip(vData) {
								This.err = nil
								continue LOOP
							}
							errData = vData
							goto errLoop
						}
						val = append(val, toV)
					}
				}
			}
			break
		default:
			continue
			break
		}
		if stmt == nil {
			stmt = This.getStmt("insert")
			if stmt == nil {
				goto errLoop
			}
		}
		_, This.conn.err = stmt.Exec(val)
		if This.conn.err != nil {
			if This.CheckDataSkip(vData) {
				This.conn.err = nil
				continue LOOP
			}
			errData = vData
			This.err = This.conn.err
		}
		if This.err != nil {
			log.Println("plugin clickhouse insert exec err:", This.err, " data:", val)
			goto errLoop
		}
	}
errLoop:
	return
}
