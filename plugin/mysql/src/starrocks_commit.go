package src

import (
	"fmt"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"log"
)

func (This *Conn) StarrocksNotAutoTableCommit(list []*pluginDriver.PluginDataType) (ErrData *pluginDriver.PluginDataType, e error) {
	This.conn.err = This.conn.Begin()
	if This.conn.err != nil {
		return nil, This.conn.err
	}
	switch This.p.SyncMode {
	case SYNCMODE_NORMAL:
		ErrData = This.CommitNormal(list)
		break
	case SYNCMODE_LOG_UPDATE:
		ErrData = This.CommitLogMod_Update(list)
		break
	case SYNCMODE_LOG_APPEND:
		ErrData = This.CommitLogMod_Append(list)
		break
	default:
		This.err = fmt.Errorf("同步模式ERROR:%s", This.p.SyncMode)
		break
	}
	if This.conn.err != nil {
		This.err = This.conn.err
		//log.Println("plugin mysql conn.err",This.err)
		return ErrData, This.err
	}
	if This.err != nil {
		This.conn.err = This.conn.Rollback()
		log.Println("plugin mysql err", This.err)
		return ErrData, This.err
	}
	This.conn.err = This.conn.Commit()
	This.StmtClose()
	if This.conn.err != nil {
		return nil, This.conn.err
	}
	return
}

// 自动创建表的提交
func (This *Conn) StarrocksAutoTableCommit(list []*pluginDriver.PluginDataType) (ErrData *pluginDriver.PluginDataType, e error) {
	dataMap := make(map[string][]*pluginDriver.PluginDataType, 0)
	var ok bool
	for _, PluginData := range list {
		key := PluginData.SchemaName + "." + PluginData.TableName
		if _, ok = dataMap[key]; !ok {
			dataMap[key] = make([]*pluginDriver.PluginDataType, 0)
		}
		dataMap[key] = append(dataMap[key], PluginData)
	}
	for _, data := range dataMap {
		p, err := This.getAutoTableFieldType(data[0])
		if err != nil {
			return data[0], e
		}
		This.p.Field = p.Field
		This.p.fieldCount = len(p.Field)
		This.p.schemaAndTable = p.SchemaAndTable
		This.p.PriKey = p.PriKey
		This.p.toPriKey = p.ToPriKey
		This.p.fromPriKey = p.FromPriKey
		This.conn.err = This.conn.Begin()
		if This.conn.err != nil {
			This.err = This.conn.err
			break
		}
		switch This.p.SyncMode {
		case SYNCMODE_NORMAL:
			ErrData = This.CommitNormal(data)
			break
		case SYNCMODE_LOG_UPDATE:
			ErrData = This.CommitLogMod_Update(data)
			break
		case SYNCMODE_LOG_APPEND:
			ErrData = This.CommitLogMod_Append(data)
			break
		default:
			This.err = fmt.Errorf("同步模式ERROR:%s", This.p.SyncMode)
			break
		}
		if This.conn.err != nil {
			This.err = This.conn.err
		}
		if This.err != nil {
			This.conn.err = This.conn.Rollback()
			return ErrData, This.err
		}
		This.conn.err = This.conn.Commit()
		This.StmtClose()
		if This.conn.err != nil {
			break
		}
	}
	return
}
