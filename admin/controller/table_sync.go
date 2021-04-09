/*
Copyright [2018] [jc3wish]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package controller

import (
	pluginStorage "github.com/brokercap/Bifrost/plugin/storage"
	"github.com/brokercap/Bifrost/server"
)

type TableSyncController struct {
	CommonController
}

type syncListStruct struct {
	DbName       string             `json:"DbName"`
	SchemaName   string             `json:"SchemaName"`
	TableName    string             `json:"TableName"`
	ToServerList []*server.ToServer `json:"ToServerList"`
}

type syncListResult struct {
	DbName          string
	SchemaName      string
	TableName       string
	ToServerKey     string
	SyncStatus      string
	SyncList        []syncListStruct
	ToServerKeyList map[string]*pluginStorage.ToServer
	DbList          map[string]server.DbListStruct
	ChannelID       int
	ChannelList     map[int]*server.Channel
}

func (c *TableSyncController) Index() {
	data := c.getList()
	c.SetTitle("SyncList")
	c.SetData("DbName", data.DbName)
	c.SetData("SchemaName", data.SchemaName)
	c.SetData("TableName", data.TableName)
	c.SetData("ToServerKey", data.ToServerKey)
	c.SetData("SyncStatus", data.SyncStatus)
	c.SetData("SyncList", data.SyncList)
	c.SetData("ToServerKeyList", data.ToServerKeyList)
	c.SetData("DbList", data.DbList)
	c.SetData("ChannelID", data.ChannelID)
	c.SetData("ChannelList", data.ChannelList)
	c.AddAdminTemplate("sync.list.html", "header.html", "footer.html")
}

func (c *TableSyncController) List() {
	data := c.getList()
	c.SetJsonData(data)
	c.StopServeJSON()
}

func (c *TableSyncController) getList() syncListResult {
	dbname := c.Ctx.Request.Form.Get("DbName")
	tablename := c.Ctx.Request.Form.Get("TableName")
	schema := c.Ctx.Request.Form.Get("SchemaName")
	ChannelID0, _ := c.Ctx.GetParamInt64("ChannelId", 0)
	ChannelID := int(ChannelID0)
	SyncStatus := c.Ctx.Request.Form.Get("SyncStatus")
	var syncList []syncListStruct

	schema0 := tansferSchemaName(schema)
	tablename0 := tansferTableName(tablename)

	if tablename != "" {
		syncList = get_syncList_by_table_name(dbname, schema0, tablename0)
	} else if schema != "" {
		syncList = get_syncList_by_schema_name(dbname, schema0, ChannelID)
	} else if dbname != "" {
		syncList = get_syncList_by_dbname(dbname, ChannelID)
	} else {
		syncList = get_syncList_all(ChannelID)
	}

	//假如传了 toserverkey 参数，则将 非 toserverkey 的列表给过滤掉
	ToServerKey := c.Ctx.Request.Form.Get("ToServerKey")
	if ToServerKey != "" {
		var filterToServerKeySyncList []syncListStruct
		filterToServerKeySyncList = make([]syncListStruct, 0)
		for _, v := range syncList {
			var b bool = false
			var tmp syncListStruct
			for _, val := range v.ToServerList {
				if val.ToServerKey == ToServerKey {
					if b == false {
						b = true
						tmp = syncListStruct{
							DbName:       v.DbName,
							SchemaName:   v.SchemaName,
							TableName:    v.TableName,
							ToServerList: make([]*server.ToServer, 0),
						}
					}
					tmp.ToServerList = append(tmp.ToServerList, val)
				}
			}
			if b {
				filterToServerKeySyncList = append(filterToServerKeySyncList, tmp)
			}
		}
		syncList = filterToServerKeySyncList
	}

	//假如传了 toserverkey 参数，则将 非 toserverkey 的列表给过滤掉
	if SyncStatus != "" {
		var filterToServerKeySyncList []syncListStruct
		filterToServerKeySyncList = make([]syncListStruct, 0)
		for _, v := range syncList {
			var tmp syncListStruct
			tmp = syncListStruct{
				DbName:       v.DbName,
				SchemaName:   v.SchemaName,
				TableName:    v.TableName,
				ToServerList: make([]*server.ToServer, 0),
			}
			for _, val := range v.ToServerList {
				switch SyncStatus {
				case "nodata":
					if val.Status == "" {
						tmp.ToServerList = append(tmp.ToServerList, val)
					}
					break
				case "running":
					if val.Status == "running" {
						tmp.ToServerList = append(tmp.ToServerList, val)
					}
					break
				case "error":
					if val.Error != "" {
						tmp.ToServerList = append(tmp.ToServerList, val)
					}
					break
				default:
					SyncStatus = ""
					break
				}
			}
			if len(tmp.ToServerList) > 0 {
				filterToServerKeySyncList = append(filterToServerKeySyncList, tmp)
			}
		}
		syncList = filterToServerKeySyncList
	}

	var ChannelList map[int]*server.Channel
	if dbname != "" {
		ChannelList = server.GetDBObj(dbname).ListChannel()
	}
	data := syncListResult{
		DbName:          dbname,
		SchemaName:      schema,
		TableName:       tablename,
		ToServerKey:     ToServerKey,
		SyncStatus:      SyncStatus,
		SyncList:        syncList,
		ChannelID:       ChannelID,
		DbList:          server.GetListDb(),
		ToServerKeyList: pluginStorage.ToServerMap,
		ChannelList:     ChannelList,
	}
	return data
}

func get_syncList_all(ChannelID int) []syncListStruct {
	syncList := make([]syncListStruct, 0)
	DBList := server.GetListDb()
	for _, db := range DBList {
		t1 := server.GetDBObj(db.Name)
		for key, table := range t1.GetTables() {
			if ChannelID > 0 && ChannelID != table.ChannelKey {
				continue
			}
			schemaName, tableName := server.GetSchemaAndTableBySplit(key)
			syncList = append(syncList, syncListStruct{db.Name, schemaName, tableName, table.ToServerList})
		}
	}
	return syncList
}

func get_syncList_by_dbname(dbname string, ChannelID int) []syncListStruct {
	syncList := make([]syncListStruct, 0)
	t1 := server.GetDBObj(dbname)
	if t1 != nil {
		for key, table := range t1.GetTables() {
			if ChannelID > 0 && ChannelID != table.ChannelKey {
				continue
			}
			schemaName, tableName := server.GetSchemaAndTableBySplit(key)
			syncList = append(syncList, syncListStruct{dbname, schemaName, tableName, table.ToServerList})
		}
	}
	return syncList
}

func get_syncList_by_schema_name(dbname, schema_name string, ChannelID int) []syncListStruct {
	syncList := make([]syncListStruct, 0)
	t1 := server.GetDBObj(dbname)
	if t1 != nil {
		for key, table := range t1.GetTables() {
			if ChannelID > 0 && ChannelID != table.ChannelKey {
				continue
			}
			schemaName, tableName := server.GetSchemaAndTableBySplit(key)
			if schemaName == schema_name {
				syncList = append(syncList, syncListStruct{dbname, schemaName, tableName, table.ToServerList})
			}
		}
	}
	return syncList
}

func get_syncList_by_table_name(dbname, schema_name, table_name string) []syncListStruct {
	syncList := make([]syncListStruct, 0)
	t1 := server.GetDBObj(dbname)
	if t1 == nil {
		return syncList
	}
	table := t1.GetTable(schema_name, table_name)
	if table == nil {
		return syncList
	}
	syncList = append(syncList, syncListStruct{dbname, schema_name, table_name, table.ToServerList})
	return syncList
}
