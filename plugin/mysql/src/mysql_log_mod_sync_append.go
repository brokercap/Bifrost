package src

/*
将所有数据转成 insert 的方式写入到 mysql
*/

import (
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	dbDriver "database/sql/driver"
)

func (This *Conn) CommitLogMod_Append(list []*pluginDriver.PluginDataType) (e error)  {
	//将update, delete,insert 的数据全转成  insert 语句
	var toV dbDriver.Value
	var stmt dbDriver.Stmt
	n  := len(list)
	for i := 0; i < n; i++ {
		data := list[i]
		switch data.EventType {
		case "update":
			val := make([]dbDriver.Value,0)
			for _,v:=range This.p.Field{
				toV,This.err = This.dataTypeTransfer(This.getMySQLData(data,1,v.FromMysqlField), v.ToField,v.ToFieldType,v.ToFieldDefault,v.ToFieldIsAutoIncrement)

				if This.err != nil{
					return This.err
				}
				val = append(val, toV)
			}
			stmt = This.getStmt(INSERT)
			if stmt == nil{
				goto errLoop
			}
			_,This.conn.err = stmt.Exec(val)
			if This.conn.err != nil{
				goto errLoop
			}
			break
		case "insert","delete":
			val := make([]dbDriver.Value,0)
			for _,v:=range This.p.Field {
				toV, This.err = This.dataTypeTransfer(This.getMySQLData(data, 0, v.FromMysqlField), v.ToField, v.ToFieldType, v.ToFieldDefault,v.ToFieldIsAutoIncrement)
				if This.err != nil {
					return This.err
				}
				val = append(val, toV)
			}
			stmt = This.getStmt(INSERT)
			if stmt == nil{
				goto errLoop
			}
			_,This.conn.err = stmt.Exec(val)
			if This.conn.err != nil{
				goto errLoop
			}
			break
		}

	}

errLoop:
	return This.conn.err
}
