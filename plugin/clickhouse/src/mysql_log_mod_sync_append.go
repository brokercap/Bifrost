package src

/*
将所有数据转成 insert 的方式写入到 mysql
*/

import (
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	dbDriver "database/sql/driver"
)

func (This *Conn) CommitLogMod_Append(list []*pluginDriver.PluginDataType,n int) (e error)  {
	var toV interface{}
	stmt := This.getStmt("insert")
	if stmt == nil {
		goto errLoop
	}
	for i := n - 1; i >= 0; i-- {
		vData := list[i]
		val := make([]dbDriver.Value, 0)
		l := len(vData.Rows)
		switch vData.EventType {
		case "insert","delete":
			for k := 0; k < l ;k++{
				for _, v := range This.p.Field {
					toV, This.err = CkDataTypeTransfer(This.getMySQLData(vData,k,v.MySQL), v.CK, v.CkType)
					if This.err != nil {
						stmt.Close()
						goto errLoop
					}
					val = append(val, toV)
				}
			}
			break
		case "update":
			for k := 0; k < l ;k++{
				if k%2 != 0 {
					for _, v := range This.p.Field {
						toV, This.err = CkDataTypeTransfer(This.getMySQLData(vData,k,v.MySQL), v.CK, v.CkType)
						if This.err != nil {
							stmt.Close()
							goto errLoop
						}
						val = append(val, toV)
					}
				}
			}
			break
		default:
			break
		}
		_, This.err = stmt.Exec(val)
		if This.err != nil {
			stmt.Close()
			goto errLoop
		}
	}
errLoop:
	return
}
