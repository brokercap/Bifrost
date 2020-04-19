/*
	普通模式同步
	update 转成 insert on update
	insert 转成 replace into
	delete 转成 delete
	只要是同一条数据，只要有遍历过，后面遍历出来的数据，则不再进行操作
*/
package src

import (
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	dbDriver "database/sql/driver"
)

func (This *Conn) CommitNormal(list []*pluginDriver.PluginDataType) (e error)  {

	//因为数据是有序写到list里的，里有 update,delete,insert，所以这里我们反向遍历

	//用于存储数据库中最后一次操作记录
	opMap := make(map[interface{}]*opLog, 0)

	//从最后一条数据开始遍历
	var toV dbDriver.Value
	var stmt dbDriver.Stmt
	n  := len(list)
	for i := n - 1; i >= 0; i-- {
		data := list[i]
		switch data.EventType {
		case "update":
			val := make([]dbDriver.Value,This.p.fieldCount*2)
			for i,v:=range This.p.Field{
				toV,This.err = This.dataTypeTransfer(This.getMySQLData(data,1,v.FromMysqlField), v.ToField,v.ToFieldType,v.ToFieldDefault)

				if This.err != nil{
					return This.err
				}
				val[i] = toV
				//第几个字段 + 总字段数量 - 1  算出，on update 所在数组中的位置
				val[i+This.p.fieldCount] = toV
			}

			if checkOpMap(opMap,data.Rows[1][This.p.mysqlPriKey], "update") == true {
				continue
			}
			stmt = This.getStmt(UPDATE)
			if stmt == nil{
				goto errLoop
			}
			_,This.conn.err = stmt.Exec(val)
			setOpMapVal(opMap,data.Rows[1][This.p.mysqlPriKey],nil,"update")
			break
		case "delete":
			where := make([]dbDriver.Value,0)
			for _,v := range This.p.PriKey{
				toV,This.err = This.dataTypeTransfer(This.getMySQLData(data,0,v.FromMysqlField), v.ToField,v.ToFieldType,v.ToFieldDefault)
				where = append(where,toV)
			}
			if checkOpMap(opMap,data.Rows[0][This.p.mysqlPriKey], "delete") == false {
				stmt = This.getStmt(DELETE)
				if stmt == nil{
					goto errLoop
				}
				_,This.conn.err = stmt.Exec(where)
				if This.conn.err != nil{
					goto errLoop
				}
				setOpMapVal(opMap,data.Rows[0][This.p.mysqlPriKey],nil,"delete")
			}
			break
		case "insert":
			val := make([]dbDriver.Value,0)
			i:=0
			for _,v:=range This.p.Field{
				toV,This.err = This.dataTypeTransfer(This.getMySQLData(data,0,v.FromMysqlField), v.ToField,v.ToFieldType,v.ToFieldDefault)
				if This.err != nil{
					return This.err
				}
				val = append(val,toV)
				i++
			}

			if checkOpMap(opMap,data.Rows[0][This.p.mysqlPriKey], "insert") == true {
				continue
			}
			stmt = This.getStmt(REPLACE_INSERT)
			if stmt == nil{
				goto errLoop
			}
			_,This.conn.err = stmt.Exec(val)
			if This.conn.err != nil{
				goto errLoop
			}
			setOpMapVal(opMap,data.Rows[0][This.p.mysqlPriKey],&val,"insert")
			break
		}

	}

errLoop:
	return This.conn.err
}