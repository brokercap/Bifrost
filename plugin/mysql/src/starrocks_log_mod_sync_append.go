package src

import (
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
)

/*
	StarRocks普通模式同步
	update 转成 insert
	insert 转成 insert
	delete 转成 insert
*/

func (This *Conn) StarRocksCommit_Append(list []*pluginDriver.PluginDataType) (errData *pluginDriver.PluginDataType) {
	var err error
	errData, err = This.StarRocksInsert(list)
	if err != nil {
		This.err, This.conn.err = err, err
	} else {
		This.err = nil
	}
	return errData
}
