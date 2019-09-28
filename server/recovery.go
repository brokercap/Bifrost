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
	"fmt"
	"strconv"
	"github.com/brokercap/Bifrost/config"
)

type dbSaveInfo struct {
	Name               		string `json:"Name"`
	ConnectUri         		string `json:"ConnectUri"`
	ConnStatus         		string `json:"ConnStatus"`
	ConnErr            		string `json:"ConnErr"`
	ChannelMap         		map[int]channelSaveInfo `json:"ChannelMap"`
	LastChannelID      		int	`json:"LastChannelID"`
	TableMap           		map[string]*Table `json:"TableMap"`
	BinlogDumpFileName 		string `json:"BinlogDumpFileName"`
	BinlogDumpPosition 		uint32 `json:"BinlogDumpPosition"`
	MaxBinlogDumpFileName 	string `json:"MaxBinlogDumpFileName"`
	MaxinlogDumpPosition 	uint32 `json:"MaxinlogDumpPosition"`
	ReplicateDoDb      		map[string]uint8 `json:"ReplicateDoDb"`
	ServerId           		uint32 `json:"ServerId"`
	AddTime            		int64 `json:"AddTime"`
}

type channelSaveInfo struct {
	Name string
	MaxThreadNum     int // 消费通道的最大线程数
	CurrentThreadNum int
	Status           string //stop ,starting,running,wait
}

func CompareBinlogPositionAndReturnGreater(BinlogFileNum1 int, BinlogPosition1 uint32,BinlogFileNum2 int,BinlogPosition2 uint32)(int,uint32){
	if BinlogFileNum1 > BinlogFileNum2{
		return BinlogFileNum1,BinlogPosition1
	}else if BinlogFileNum1 == BinlogFileNum2{
		if BinlogPosition1 >= BinlogPosition2 {
			return BinlogFileNum1, BinlogPosition1
		}else{
			return BinlogFileNum2, BinlogPosition2
		}
	}else{
		return BinlogFileNum2, BinlogPosition2
	}
}

func CompareBinlogPositionAndReturnLess(BinlogFileNum1 int, BinlogPosition1 uint32,BinlogFileNum2 int,BinlogPosition2 uint32)(int,uint32){
	if BinlogFileNum1 > BinlogFileNum2{
		return BinlogFileNum2,BinlogPosition2
	}else if BinlogFileNum1 == BinlogFileNum2{
		if BinlogPosition1 >= BinlogPosition2 {
			return BinlogFileNum2, BinlogPosition2
		}else{
			return BinlogFileNum1, BinlogPosition1
		}
	}else{
		return BinlogFileNum1, BinlogPosition1
	}
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
		db := AddNewDB(name, dbInfo.ConnectUri, dbInfo.BinlogDumpFileName, dbInfo.BinlogDumpPosition, dbInfo.ServerId,dbInfo.MaxBinlogDumpFileName,dbInfo.MaxinlogDumpPosition,dbInfo.AddTime)
		if db == nil{
			log.Println("recovry data error2,data:",dbInfo)
			os.Exit(1)
		}
		m := make([]string, 0)
		for schemaName,_:= range dbInfo.ReplicateDoDb{
			m = append(m, schemaName)
		}
		db.SetReplicateDoDb(m)
		for oldChannelId,cInfo := range dbInfo.ChannelMap{
			ch,ChannelID := db.AddChannel(cInfo.Name,cInfo.MaxThreadNum)
			ch.Status = cInfo.Status
			channelIDMap[oldChannelId] = ChannelID
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

		// 二进制文件格式是xxx.000001 ,后面数字是6位数，不够前面补0
		index := strings.IndexAny(dbInfo.BinlogDumpFileName, ".")
		binlogPrefix := dbInfo.BinlogDumpFileName[0:index]

		var PerformanceTestingFileName string = ""
		var PerformanceTestingFileNum int = 0
		var PerformanceTestingPosition uint32 = 0
		//假如在性能测试的配置文件中找到这个 找这个DbName，则直接加载配置文件中的位点
		//并且只有配置合法，才可以通过
		if PerformanceTestingParam := config.GetConfigVal("PerformanceTesting",dbInfo.Name);PerformanceTestingParam != ""{
			log.Println("PerformanceTesting",dbInfo.Name,"PerformanceTestingParam:",PerformanceTestingParam)
			t := strings.Split(PerformanceTestingParam,",")
			if len(t) == 2{
				var err error
				index := strings.IndexAny(t[0], ".")
				binlogPrefix2 := t[0][0:index]
				PerformanceTestingFileNum,err = strconv.Atoi(t[0][index+1:])
				if err == nil {
					//配置文件 和  原有DbName里设置的binlog 前缀必须一致
					if binlogPrefix2 == binlogPrefix {
						//位点也必须是数字
						position, err := strconv.Atoi(t[1])
						if err == nil && position >= 4 {
							PerformanceTestingFileName = t[0]
							PerformanceTestingPosition = uint32(position)
						}else{
							log.Println("PerformanceTesting",dbInfo.Name,err)
						}
					}else{
						log.Println("PerformanceTesting",dbInfo.Name,"binlog prefix:",binlogPrefix2," not ==",binlogPrefix)
					}
				}else{
					log.Println("PerformanceTesting",dbInfo.Name,t[0][index+1:],"not int",err)
				}
			}else{
				log.Println("PerformanceTesting",dbInfo.Name,"PerformanceTestingParam:",PerformanceTestingParam,"split error")
			}
		}

		var BinlogFileNum int = 0
		var BinlogPosition uint32 = 0
		var lastAllToServerNoraml bool = true
		if len(dbInfo.TableMap) > 0 {
			for tKey, tInfo := range dbInfo.TableMap {
				schemaName,tableName := GetSchemaAndTableBySplit(tKey)
				db.AddTable(schemaName, tableName, channelIDMap[tInfo.ChannelKey],tInfo.LastToServerID)
				for _, toServer := range tInfo.ToServerList {
					toServerBinlogPosition,_ := getBinlogPosition(getToServerBinlogkey(db,toServer))
					if toServerBinlogPosition != nil{
						toServer.BinlogFileNum,toServer.BinlogPosition  = CompareBinlogPositionAndReturnGreater(
							toServer.BinlogFileNum,toServer.BinlogPosition,
							toServerBinlogPosition.BinlogFileNum,toServerBinlogPosition.BinlogPosition)
					}
					toServerLastBinlogPosition,_ := getBinlogPosition(getToServerLastBinlogkey(db,toServer))
					if toServerLastBinlogPosition != nil{
						toServer.LastBinlogFileNum,toServer.LastBinlogPosition  = CompareBinlogPositionAndReturnGreater(
							toServer.LastBinlogFileNum,toServer.LastBinlogPosition,
							toServerLastBinlogPosition.BinlogFileNum,toServerLastBinlogPosition.BinlogPosition)
					}

					//假如是性能测试，能强制修改为指定位点
					if PerformanceTestingFileName != ""{
						toServer.BinlogFileNum = PerformanceTestingFileNum
						toServer.BinlogPosition = PerformanceTestingPosition
						toServer.LastBinlogFileNum = PerformanceTestingFileNum
						toServer.LastBinlogPosition = PerformanceTestingPosition
					}

					db.AddTableToServer(schemaName, tableName,
						&ToServer{
							ToServerID:			toServer.ToServerID,
							MustBeSuccess:  	toServer.MustBeSuccess,
							FilterQuery:  		toServer.FilterQuery,
							FilterUpdate:  		toServer.FilterUpdate,
							ToServerKey:    	toServer.ToServerKey,
							PluginName:   		toServer.PluginName,
							FieldList:      	toServer.FieldList,
							BinlogFileNum:  	toServer.BinlogFileNum,
							BinlogPosition: 	toServer.BinlogPosition,
							PluginParam:    	toServer.PluginParam,
							LastBinlogPosition:	toServer.LastBinlogPosition,
							LastBinlogFileNum: 	toServer.LastBinlogFileNum,
						})

					//假如当前同步配置 最后输入的 位点 等于 最后成功的位点，说明当前这个 同步配置的位点是没有问题的
					if toServer.BinlogFileNum == toServer.LastBinlogFileNum && toServer.BinlogPosition == toServer.LastBinlogPosition{
						if lastAllToServerNoraml == false{
							//假如有一个同步不太正常的情况下，取小值
							//这里用判断 BinlogFileNum == 0 是因为 绝对不会出现，因为绝对 第一次循环 lastAllToServerNoraml == true
							BinlogFileNum1,BinlogPosition1  := CompareBinlogPositionAndReturnLess(
								BinlogFileNum,BinlogPosition,
								toServer.BinlogFileNum,toServer.BinlogPosition)
							if BinlogFileNum1 == BinlogFileNum && BinlogPosition1 == BinlogPosition{

							}else{
								log.Println("recovery binlog change:",dbInfo.Name, " old:",BinlogFileNum," ",BinlogPosition, " new:",BinlogFileNum1," ",BinlogPosition1)
								BinlogFileNum = BinlogFileNum1
								BinlogPosition = BinlogPosition1
							}

						}else{
							//假如所有表都还是正常同步的情况下，取大值
							BinlogFileNum1, BinlogPosition1 := CompareBinlogPositionAndReturnGreater(
								BinlogFileNum, BinlogPosition,
								toServer.BinlogFileNum, toServer.BinlogPosition)

							if BinlogFileNum1 == BinlogFileNum && BinlogPosition1 == BinlogPosition{

							}else{
								log.Println("recovery binlog change:",dbInfo.Name, " old:",BinlogFileNum," ",BinlogPosition, " new:",BinlogFileNum1," ",BinlogPosition1)
								BinlogFileNum = BinlogFileNum1
								BinlogPosition = BinlogPosition1
							}
						}
						continue
					}else{
						lastAllToServerNoraml = false
					}

					if toServer.LastBinlogFileNum == 0 && toServer.BinlogFileNum > 0{
						saveBinlogPosition(getToServerLastBinlogkey(db,toServer),toServer.BinlogFileNum,toServer.BinlogPosition)
					}

					//取大值
					BinlogFileNum1, BinlogPosition1 := CompareBinlogPositionAndReturnGreater(
						BinlogFileNum, BinlogPosition,
						toServer.BinlogFileNum, toServer.BinlogPosition)

					if BinlogFileNum1 == BinlogFileNum && BinlogPosition1 == BinlogPosition{

					}else{
						log.Println("recovery binlog change:",dbInfo.Name, " old:",BinlogFileNum," ",BinlogPosition, " new:",BinlogFileNum1," ",BinlogPosition1)
						BinlogFileNum = BinlogFileNum1
						BinlogPosition = BinlogPosition1
					}
				}
			}
		}

		// 拿镜像数据里的 位点和level中存储 db 位点对比，取更大的值
		// 因为镜像数据只有配置更改了才会修改，但是 level中的数据是只要有数据同步 及 每5秒强制刷一次
		LastDBBinlogFileNum,_ := strconv.Atoi(dbInfo.BinlogDumpFileName[index+1:])
		DBBinlogKey := getDBBinlogkey(db)
		DBLastBinlogPosition,_ := getBinlogPosition(DBBinlogKey)
		if DBLastBinlogPosition != nil{
			//假如key val存储中DB 的位点值存在 取大值
			LastDBBinlogFileNum1, binlogDumpPosition1 := CompareBinlogPositionAndReturnGreater(
				LastDBBinlogFileNum, db.binlogDumpPosition,
				DBLastBinlogPosition.BinlogFileNum, DBLastBinlogPosition.BinlogPosition)
			if LastDBBinlogFileNum == LastDBBinlogFileNum1 && db.binlogDumpPosition == binlogDumpPosition1{

			}else{
				log.Println("recovery binlog change:",dbInfo.Name, " old:",LastDBBinlogFileNum," ",db.binlogDumpPosition, " new:",LastDBBinlogFileNum1," ",db.binlogDumpPosition)
				LastDBBinlogFileNum = LastDBBinlogFileNum1
				db.binlogDumpPosition = binlogDumpPosition1
			}
		}
		db.binlogDumpFileName = binlogPrefix+"."+fmt.Sprintf("%06d",LastDBBinlogFileNum)


		//假如所有表数据同步都是正常的，则取 db 里的位点配置，否则取 同步表里最小位点
		//假如有一个表的数据同步位点不正常,则取不正常位点的最小值,否则取和当前db最后保存的位 及表位点的最大值
		if lastAllToServerNoraml == false{
			db.binlogDumpFileName = binlogPrefix+"."+fmt.Sprintf("%06d",BinlogFileNum)
			db.binlogDumpPosition = BinlogPosition
		}else{
			//这里为什么要取大值,是因为位点是定时刷盘的,有可能在哪些特殊情况下,表位点成功了,db位点没保存成功
			LastDBBinlogFileNum1, binlogDumpPosition1 := CompareBinlogPositionAndReturnGreater(
				LastDBBinlogFileNum, db.binlogDumpPosition,
				BinlogFileNum, BinlogPosition)
			db.binlogDumpFileName = binlogPrefix+"."+fmt.Sprintf("%06d",LastDBBinlogFileNum)

			if LastDBBinlogFileNum == LastDBBinlogFileNum1 && db.binlogDumpPosition == binlogDumpPosition1{

			}else{
				log.Println("recovery binlog change:",dbInfo.Name, " old:",LastDBBinlogFileNum," ",db.binlogDumpPosition, " new:",LastDBBinlogFileNum1," ",db.binlogDumpPosition)
				LastDBBinlogFileNum = LastDBBinlogFileNum1
				db.binlogDumpPosition = binlogDumpPosition1
			}
		}

		//如果是性能测试配置，强制修改位点
		if PerformanceTestingFileName != ""{
			db.binlogDumpFileName = PerformanceTestingFileName
			db.binlogDumpPosition = PerformanceTestingPosition
		}

		if dbInfo.ConnStatus == "closing"{
			dbInfo.ConnStatus = "close"
		}
		if dbInfo.ConnStatus != "close" && dbInfo.ConnStatus != "stop"{
			if dbInfo.BinlogDumpFileName != dbInfo.MaxBinlogDumpFileName && dbInfo.BinlogDumpPosition != dbInfo.MaxinlogDumpPosition{
				go db.Start()
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
			ConnectUri:				db.ConnectUri,
			ConnStatus:				db.ConnStatus,
			LastChannelID:			db.LastChannelID,
			BinlogDumpFileName:		db.binlogDumpFileName,
			BinlogDumpPosition:		db.binlogDumpPosition,
			MaxBinlogDumpFileName:	db.maxBinlogDumpFileName,
			MaxinlogDumpPosition:	db.maxBinlogDumpPosition,
			ReplicateDoDb:			db.replicateDoDb,
			ServerId:				db.serverId,
			ChannelMap:				make(map[int]channelSaveInfo,0),
			TableMap:				db.tableMap,
			AddTime:				db.AddTime,
		}
		for chid, c := range db.channelMap{
			c.Lock()
			data[k].ChannelMap[chid] = channelSaveInfo{
				Name:				c.Name,
				MaxThreadNum:		c.MaxThreadNum,
				CurrentThreadNum:	0,
				Status:				c.Status,
			}
			c.Unlock()
		}
		db.Unlock()
	}
	DbLock.Unlock()
	return data
}
