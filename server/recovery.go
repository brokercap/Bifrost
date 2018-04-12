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
package server

import (
	"os"
	"encoding/json"
	"log"
	"strings"
)

type dbSaveInfo struct {
	Name               string `json:"Name"`
	ConnectUri         string `json:"ConnectUri"`
	ConnStatus         string `json:"ConnStatus"`
	ConnErr            string `json:"ConnErr"`
	ChannelMap         map[int]channelSaveInfo `json:"ChannelMap"`
	LastChannelID      int	`json:"LastChannelID"`
	TableMap           map[string]*Table `json:"TableMap"`
	BinlogDumpFileName string `json:"BinlogDumpFileName"`
	BinlogDumpPosition uint32 `json:"BinlogDumpPosition"`
	MaxBinlogDumpFileName string `json:"MaxBinlogDumpFileName"`
	MaxinlogDumpPosition uint32 `json:"MaxinlogDumpPosition"`
	ReplicateDoDb      map[string]uint8 `json:"ReplicateDoDb"`
	ServerId           uint32 `json:"ServerId"`
}

type channelSaveInfo struct {
	Name string
	MaxThreadNum     int // 消费通道的最大线程数
	CurrentThreadNum int
	Status           string //stop ,starting,running,wait
}

func Recovery(content *json.RawMessage){
	var data map[string]dbSaveInfo

	errors := json.Unmarshal(*content,&data)
	if errors != nil{
		log.Println( "recorery db content errors;",errors)
		os.Exit(1)
		return
	}
	recoveryData(data)
}

func recoveryData(data map[string]dbSaveInfo){
	for name,dbInfo :=range data{
		channelIDMap := make(map[int]int,0)
		db := AddNewDB(name, dbInfo.ConnectUri, dbInfo.BinlogDumpFileName, dbInfo.BinlogDumpPosition, dbInfo.ServerId,dbInfo.MaxBinlogDumpFileName,dbInfo.MaxinlogDumpPosition)
		if db == nil{
			log.Println("recovry data error2,data:",dbInfo)
			os.Exit(1)
		}
		m := make([]string, 0)
		for schemaName,_:= range dbInfo.ReplicateDoDb{
			m = append(m, schemaName)
		}
		db.SetReplicateDoDb(m)
		i := 1
		for oldChannelId,cInfo := range dbInfo.ChannelMap{
			ch := db.AddChannel(cInfo.Name,cInfo.MaxThreadNum)
			ch.Status = cInfo.Status
			channelIDMap[oldChannelId] = i
			i++
			if cInfo.Status != "stop" && cInfo.Status != "close" {
				if dbInfo.ConnStatus != "close" || dbInfo.ConnStatus != "stop" {
					if dbInfo.BinlogDumpFileName != dbInfo.MaxBinlogDumpFileName && dbInfo.BinlogDumpPosition != dbInfo.MaxinlogDumpPosition{
						ch.Start()
						continue
					}
				}
			}
			//db close,channel must be close
			ch.Close()
		}
		for tKey,tInfo := range dbInfo.TableMap{
			i := strings.IndexAny(tKey, "-")
			schemaName := tKey[0:i]
			tableName := tKey[i+1:]
			db.AddTable(schemaName, tableName, channelIDMap[tInfo.ChannelKey])
			for _,toServer := range tInfo.ToServerList{
				db.AddTableToServer(schemaName, tableName,
					ToServer{
						MustBeSuccess:toServer.MustBeSuccess,
						Type:          toServer.Type,
						DataType:	   toServer.DataType,
						KeyConfig:     toServer.KeyConfig,
						ValueConfig:   toServer.ValueConfig,
						ToServerKey:   toServer.ToServerKey,
						AddEventType:  toServer.AddEventType,
						AddSchemaName: toServer.AddSchemaName,
						AddTableName:  toServer.AddTableName,
						Expir:		   toServer.Expir,
					})
			}
		}
		if dbInfo.ConnStatus != "close" && dbInfo.ConnStatus != "stop"{
			if dbInfo.BinlogDumpFileName != dbInfo.MaxBinlogDumpFileName && dbInfo.BinlogDumpPosition != dbInfo.MaxinlogDumpPosition{
				db.Start()
			}
		}
	}
}


func StopAllChannel(){
	DbLock.Lock()
	for _,db := range DbList{
		db.killStatus = 1
		func(){
			defer func() {
				if 	err := recover();err!=nil{
					return
				}
			}()
			db.binlogDump.KillDump()
		}()
	}
	DbLock.Unlock()
}

func SaveDBInfoToFileData() interface{}{
	DbLock.Lock()
	var data map[string]dbSaveInfo
	data = make(map[string]dbSaveInfo,0)
	for k,db := range DbList{
		db.Lock()
		data[k] = dbSaveInfo{
			Name:db.Name,
			ConnectUri:db.ConnectUri,
			ConnStatus:db.ConnStatus,
			LastChannelID:db.LastChannelID,
			BinlogDumpFileName:db.binlogDumpFileName,
			BinlogDumpPosition:db.binlogDumpPosition,
			MaxBinlogDumpFileName:db.maxBinlogDumpFileName,
			MaxinlogDumpPosition:db.maxBinlogDumpPosition,
			ReplicateDoDb:db.replicateDoDb,
			ServerId:db.serverId,
			ChannelMap:make(map[int]channelSaveInfo,0),
			TableMap:db.tableMap,
		}
		for chid, c := range db.channelMap{
			c.Lock()
			data[k].ChannelMap[chid] = channelSaveInfo{
				Name:c.Name,
				MaxThreadNum:c.MaxThreadNum,
				CurrentThreadNum:0,
				Status:c.Status,
			}
			c.Unlock()
		}
		db.Unlock()
	}
	DbLock.Unlock()
	return data
}
