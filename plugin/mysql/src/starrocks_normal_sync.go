package src

import (
	"fmt"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"log"
)

/*
	StarRocks普通模式同步
	update 转成 insert
	insert 转成 insert
	delete 转成 delete
	只要是同一条数据，只要有遍历过，后面遍历出来的数据，则不再进行操作
*/

func (This *Conn) StarRocksCommitNormal(list []*pluginDriver.PluginDataType) (errData *pluginDriver.PluginDataType) {
	var err error
	//因为数据是有序写到list里的，里有 update,delete,insert，所以这里我们反向遍历

	//用于存储数据库中最后一次操作记录
	opMap := make(map[interface{}]*opLog, 0)
	insertList := make([]*pluginDriver.PluginDataType, 0)
	deleteList := make([][]string, 0)
	//从最后一条数据开始遍历
	n := len(list)
LOOP:
	for i := n - 1; i >= 0; i-- {
		data := list[i]
		if This.CheckDataSkip(data) {
			This.conn.err = nil
			continue LOOP
		}
		switch data.EventType {
		case "update", "insert":
			k := len(data.Rows) - 1
			if checkOpMap(opMap, data.Rows[k][This.p.fromPriKey], data.EventType) == true {
				continue
			}
			setOpMapVal(opMap, data.Rows[k][This.p.fromPriKey], nil, data.EventType)
			insertList = append(insertList, data)
			break
		case "delete":
			priKey := data.Rows[0][This.p.fromPriKey]
			if priKey == nil {
				continue
			}
			if checkOpMap(opMap, data.Rows[0][This.p.fromPriKey], "delete") == false {
				setOpMapVal(opMap, data.Rows[0][This.p.fromPriKey], nil, "delete")
				deleteList = append(deleteList, []string{fmt.Sprint(priKey)})
			}
			break
		default:
			continue
		}
	}
	if len(deleteList) > 0 {
		err = This.StarRocksDelete(This.GetSchemaName(list[0]), This.GetTableName(list[0]), list[0].Pri, deleteList)
		if err != nil {
			This.err, This.conn.err = err, err
			log.Printf("[ERROR] output[%s] StarRocksCommitNormal delete:(%+v) SchemaName:%s TableName:%s err:%+v", OutputName, deleteList, list[0].SchemaName, list[0].TableName, err)
			return nil
		} else {
			This.err = nil
		}
	}
	if len(insertList) > 0 {
		errData, err = This.StarRocksInsert(insertList)
		if err != nil {
			This.err, This.conn.err = err, err
			log.Printf("[ERROR] output[%s] StarRocksCommitNormal insert SchemaName:%s TableName:%s err:%+v", OutputName, list[0].SchemaName, list[0].TableName, err)
			return errData
		} else {
			This.err = nil
		}
	}
	return nil
}
