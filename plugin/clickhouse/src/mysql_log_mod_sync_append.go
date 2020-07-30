package src

/*
将所有数据转成 insert 的方式写入到 mysql
*/

import (
	dbDriver "database/sql/driver"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"log"
)

func (This *Conn) CommitLogMod_Append(list []*pluginDriver.PluginDataType,n int) (e error)  {
	stmt := This.getStmt("insert")
	if stmt == nil {
		goto errLoop
	}
	defer stmt.Close()
	for i := 0; i < n; i++ {
		vData := list[i]
		val := make([]dbDriver.Value, 0)
		l := len(vData.Rows)
		switch vData.EventType {
		case "insert","delete":
			for k := 0; k < l ;k++{
				for _, v := range This.p.Field {
					var toV interface{}
					toV, This.err = CkDataTypeTransfer(This.getMySQLData(vData,k,v.MySQL), v.CK, v.CkType)
					if This.err != nil {
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
						var toV interface{}
						toV, This.err = CkDataTypeTransfer(This.getMySQLData(vData,k,v.MySQL), v.CK, v.CkType)
						if This.err != nil {
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
		_, This.err = stmt.Exec(val)
		if This.err != nil {
			log.Println("plugin clickhouse insert exec err:",This.err," data:",val)
			goto errLoop
		}
	}
errLoop:
	return
}
