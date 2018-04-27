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
	"fmt"
	"log"
	"time"
	"strings"
)

import (
	"github.com/Bifrost/toserver/driver"
	"github.com/Bifrost/Bristol/mysql"
	"github.com/Bifrost/toserver"
	dataDriver "database/sql/driver"
	"regexp"
	"github.com/Bifrost/server/count"
	"unsafe"
)

const RegularxEpression  = `\{\$([a-zA-Z0-9\-\_]+)\}`

func evenTypeName(e mysql.EventType) string {
	switch e {
	case mysql.WRITE_ROWS_EVENTv0, mysql.WRITE_ROWS_EVENTv1, mysql.WRITE_ROWS_EVENTv2:
		return "insert"
	case mysql.UPDATE_ROWS_EVENTv0, mysql.UPDATE_ROWS_EVENTv1, mysql.UPDATE_ROWS_EVENTv2:
		return "update"
	case mysql.DELETE_ROWS_EVENTv0, mysql.DELETE_ROWS_EVENTv1, mysql.DELETE_ROWS_EVENTv2:
		return "delete"
	}
	return fmt.Sprintf("%d", e)
}

type consume_channel_obj struct {
	db      *db
	c       *Channel
	connMap map[string]driver.ConnFun
	ToserverKey *string
	rowIndex  int
}

func NewConsumeChannel(c *Channel) *consume_channel_obj {
	return &consume_channel_obj{
		db:      c.db,
		c:       c,
		connMap: make(map[string]driver.ConnFun, 0),
		rowIndex:0,
	}
}

func (This *consume_channel_obj) checkChannleStatus() {
	if This.c.Status == "close"{
		panic("channel closed")
	}
}

func (This *consume_channel_obj) sendToServer(Type string,KeyConfig *string, ValConfig *interface{}) (result bool,err error){
	defer func() {
		if err2 := recover();err2!=nil{
			result = false
			err = fmt.Errorf(fmt.Sprint(err2))
			log.Println("sendToServer err:",err2)
		}
	}()
	switch Type {
	case "insert":
		result,err = This.connMap[*This.ToserverKey].Insert(*KeyConfig, *ValConfig)
			break
	case "update":
		result,err = This.connMap[*This.ToserverKey].Update(*KeyConfig, *ValConfig)
		break
	case "del":
		result,err = This.connMap[*This.ToserverKey].Del(*KeyConfig)
		break
	case "list":
		result,err = This.connMap[*This.ToserverKey].SendToList(*KeyConfig, *ValConfig)
		break
	}
	return
}


func (This *consume_channel_obj) consume_channel() {
	c := This.c
	var data mysql.EventReslut
	//log.Println(time.Now().Format("2006-01-02 15:04:05"))

	for {
		select {
		case data = <-This.c.chanName:
			key := data.SchemaName + "-" + data.TableName
			This.db.Lock()
			if This.db.killStatus == 1{
				This.db.Unlock()
				break
			}
			toServerList := This.db.tableMap[key].ToServerList[0:]
			This.db.Unlock()
			var result bool
			var errs error
			//var KeyConfig1, ValConfig1 string = ""
			This.checkChannleStatus()
			This.rowIndex = len(data.Rows) - 1
			for _, toServer := range toServerList {
				ToServerKey := toServer.ToServerKey
				if _, ok := This.connMap[ToServerKey]; !ok {
					This.connMap[ToServerKey] = toserver.Start(ToServerKey)
				}
				var KeyConfig string
				var ValConfig interface{}

				//Header
				switch data.Header.EventType {
				case mysql.DELETE_ROWS_EVENTv0, mysql.DELETE_ROWS_EVENTv1, mysql.DELETE_ROWS_EVENTv2:
					if toServer.Type == "list" {
						//设置ToServerKey msg 过期信息，至于是否数据是否会过期处理，具体由toServer决定
						This.connMap[ToServerKey].SetExpir(toServer.Expir)
						KeyConfig, ValConfig = This.transferData(&data, &key, &toServer)
					} else {
						KeyConfig = This.transfeResult(toServer.KeyConfig, &data)
					}
					break
				default:
					//设置ToServerKey msg 过期信息，至于是否数据是否会过期处理，具体由toServer决定
					This.connMap[ToServerKey].SetExpir(toServer.Expir)
					KeyConfig, ValConfig = This.transferData(&data, &key, &toServer)
					break
				}

				var fordo int = 0
				var lastErrId int = 0
				This.connMap[ToServerKey].SetMustBeSuccess(toServer.MustBeSuccess)
				This.ToserverKey = &ToServerKey
				for {
					result = false
					errs = nil
					if toServer.Type == "set" {
						switch data.Header.EventType {
						case mysql.WRITE_ROWS_EVENTv0, mysql.WRITE_ROWS_EVENTv1, mysql.WRITE_ROWS_EVENTv2:
							result,errs = This.sendToServer("insert",&KeyConfig,&ValConfig)
							break
						case mysql.UPDATE_ROWS_EVENTv0, mysql.UPDATE_ROWS_EVENTv1, mysql.UPDATE_ROWS_EVENTv2:
							result,errs = This.sendToServer("update",&KeyConfig,&ValConfig)
							break
						case mysql.DELETE_ROWS_EVENTv0, mysql.DELETE_ROWS_EVENTv1, mysql.DELETE_ROWS_EVENTv2:
							result,errs = This.sendToServer("del",&KeyConfig,nil)
							break
						default:
							break
						}
					} else {
						result,errs = This.sendToServer("list",&KeyConfig,&ValConfig)
					}
					if toServer.MustBeSuccess == true {
						if result == true{
							if lastErrId > 0 {
								c.DelWaitError(lastErrId)
								lastErrId = 0
							}
							break
						} else {
							if lastErrId > 0{
								dealStatus := c.GetWaitErrorDeal(lastErrId)
								if dealStatus == -1{
									lastErrId = 0
									break
								}
								if dealStatus == 1{
									c.DelWaitError(lastErrId)
									lastErrId = 0
									break
								}
							}else{
								lastErrId = c.AddWaitError(errs,data)
							}
						}
						fordo++
						if fordo==3{
							This.checkChannleStatus()
							time.Sleep(2 * time.Second)
							fordo = 0
						}
					}
				}
			}
			This.db.Lock()
			This.db.binlogDumpFileName = data.BinlogFileName
			This.db.binlogDumpPosition = data.Header.LogPos
			if This.db.killStatus == 1{
				This.db.Unlock()
				break
			}
			This.db.Unlock()
			c.countChan <- &count.FlowCount{
				//Time:"",
				Count:1,
				TableId:&key,
				ByteSize:int64(unsafe.Sizeof(data.Rows))*int64(len(toServerList)),
			}
		case <-time.After(5 * time.Second):
			//log.Println(time.Now().Format("2006-01-02 15:04:05"))
			//log.Println("count:",count)
		}
		for {
			if c.Status == "stop" {
				time.Sleep(1 * time.Second)
			} else {
				break
			}
		}
		c.Lock()
		if c.CurrentThreadNum > c.MaxThreadNum || c.Status == "close" {
			c.CurrentThreadNum--
			c.Unlock()
			break
		} else {
			c.Unlock()
		}
	}
}


func (This *consume_channel_obj) transfeResult(val string, data *mysql.EventReslut) string {
	r, _ := regexp.Compile(RegularxEpression)
	p := r.FindAllStringSubmatch(val, -1)
	for _, v := range p {
		switch v[1] {
		case "TableName":
			val = strings.Replace(val, "{$TableName}", data.TableName, -1)
			break
		case "SchemaName":
			val = strings.Replace(val, "{$SchemaName}", data.SchemaName, -1)
			break
		case "EventType":
			val = strings.Replace(val, "{EventType}", evenTypeName(data.Header.EventType), -1)
			break
		default:
			val = strings.Replace(val, v[0], fmt.Sprint(data.Rows[This.rowIndex][v[1]]), -1)
			break
		}
	}
	return val
}

func (This *consume_channel_obj) transferData(data *mysql.EventReslut, key *string, toServer *ToServer) (string, interface{}) {
	Row := data.Rows[This.rowIndex]
	var keyResult string
	var valResult interface{}
	if toServer.DataType == "string" {
		keyResult = This.transfeResult(toServer.KeyConfig, data)
		if toServer.ValueConfig == "" {
			valResult = ""
		} else {
			valResult = This.transfeResult(toServer.ValueConfig, data)
		}
		return keyResult, valResult
	}

	if toServer.DataType == "json" {
		keyResult = This.transfeResult(toServer.KeyConfig, data)
		if len(toServer.FieldList) == 0 {
			if toServer.AddEventType == true {
				Row["EventType"] = evenTypeName(data.Header.EventType)
			}
			if toServer.AddSchemaName == true {
				Row["SchemaName"] = data.SchemaName
			}
			if toServer.AddTableName == true {
				Row["TableName"] = data.TableName
			}
			valResult = Row
		} else {
			m := make(map[string]dataDriver.Value, 0)
			for _, name := range toServer.FieldList {
				if _, ok := Row[name]; !ok {
					m[name] = ""
				} else {
					m[name] = Row[name]
				}
			}
			if toServer.AddEventType == true {
				m["EventType"] = evenTypeName(data.Header.EventType)
			}
			if toServer.AddSchemaName == true {
				m["SchemaName"] = data.SchemaName
			}
			if toServer.AddTableName == true {
				m["TableName"] = data.TableName
			}
			valResult = m
		}
		return keyResult, valResult
	}

	return "", nil

}
