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

package mongo

import (
	"context"
	"fmt"
	inputDriver "github.com/brokercap/Bifrost/input/driver"
	"github.com/rwynn/gtm/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

func (c *MongoInput) GetSchemaList() (data []string, err error) {
	client, err := CreateMongoClient(c.inputInfo.ConnectUri, nil)
	if err != nil {
		return nil, err
	}
	defer client.Disconnect(nil)
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	data, err = client.ListDatabaseNames(ctx, bson.M{})
	return
}

func (c *MongoInput) GetSchemaTableList(schema string) (tableList []inputDriver.TableList, err error) {
	client, err := CreateMongoClient(c.inputInfo.ConnectUri, nil)
	if err != nil {
		return nil, err
	}
	defer client.Disconnect(nil)
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	var tableNameList []string
	tableNameList, err = client.Database(schema).ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return
	}
	tableList = make([]inputDriver.TableList, len(tableNameList))
	for i, name := range tableNameList {
		tableList[i] = inputDriver.TableList{
			TableName: name,
		}
	}
	return
}

func (c *MongoInput) GetSchemaTableFieldList(schema string, table string) (tableList []inputDriver.TableFieldInfo, err error) {
	client, err := CreateMongoClient(c.inputInfo.ConnectUri, nil)
	if err != nil {
		return nil, err
	}
	defer client.Disconnect(nil)
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	var data bson.M
	err = client.Database(schema).Collection(table).FindOne(ctx, bson.M{}, &options.FindOneOptions{Sort: bson.M{"_id": -1}}).Decode(&data)

	for k, v := range data {
		var ColumnName = k
		var ColumnType string
		var ColumnKey string
		var DataType string
		var IsNullable = true
		// _id 以varchar返回，在同步过程中会进行hex进行转成string
		// 因为mongo数据结构是非固定的，所以只要非int类型的，全以json数据类型返回
		switch v.(type) {
		case int, int64, int8, int16, int32:
			ColumnType = "bigint(20)"
			DataType = "bigint"
		case uint, uint64, uint8, uint16, uint32:
			ColumnType = "int(20) unsigned"
			DataType = "bigint"
		case time.Time:
			ColumnType = "timestamp"
			DataType = "timestamp"
		default:
			if k == "_id" {
				ColumnType = "varchar"
				ColumnKey = "PRI"
				DataType = "varchar"
				IsNullable = false
			} else {
				ColumnType = "json"
				DataType = "json"
			}
		}
		fieldInfo := inputDriver.TableFieldInfo{
			ColumnName:       &ColumnName,
			ColumnDefault:    nil,
			IsNullable:       IsNullable,
			ColumnType:       &ColumnType,
			IsAutoIncrement:  false,
			Comment:          nil,
			DataType:         &DataType,
			NumericPrecision: nil,
			NumericScale:     nil,
			ColumnKey:        &ColumnKey,
		}
		// 假如是 _id 则放到数组中第一个，主要是用户在界面上看的时候，更直观显示
		if k == "_id" {
			tableList2 := []inputDriver.TableFieldInfo{fieldInfo}
			tableList2 = append(tableList2, tableList...)
			tableList = tableList2
		} else {
			tableList = append(tableList, fieldInfo)
		}
	}
	return
}

func (c *MongoInput) CheckPrivileg() error {
	return nil
}

func (c *MongoInput) CheckUri(CheckPrivileg bool) (result inputDriver.CheckUriResult, err error) {
	var client *mongo.Client
	client, err = CreateMongoClient(c.inputInfo.ConnectUri, nil)
	if err != nil {
		return
	}
	defer client.Disconnect(nil)
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err = client.Ping(ctx, nil)
	if err != nil {
		return
	}
	result = inputDriver.CheckUriResult{
		BinlogFile:     DefaultBinlogFileName,
		BinlogPosition: 0,
		BinlogFormat:   "row",
		BinlogRowImage: "full",
		Gtid:           BatchAndReplicate,
		ServerId:       1,
	}
	return
}

func (c *MongoInput) GetCurrentPosition() (p *inputDriver.PluginPosition, err error) {
	var client *mongo.Client
	client, err = CreateMongoClient(c.inputInfo.ConnectUri, nil)
	if err != nil {
		return
	}
	defer client.Disconnect(nil)
	var lastOpLogPosition primitive.Timestamp
	lastOpLogPosition, err = gtm.LastOpTimestamp(client, gtm.DefaultOptions())
	if err != nil {
		return
	}
	p = &inputDriver.PluginPosition{
		GTID:           c.OpLogPosition2GTID(&lastOpLogPosition),
		BinlogFileName: c.inputInfo.BinlogFileName,
		BinlogPostion:  c.inputInfo.BinlogPostion,
		Timestamp:      lastOpLogPosition.T,
		EventID:        c.eventID,
	}
	return
}

func (c *MongoInput) GetVersion() (version string, err error) {
	var client *mongo.Client
	client, err = CreateMongoClient(c.inputInfo.ConnectUri, nil)
	if err != nil {
		return
	}
	defer client.Disconnect(nil)
	var buildInfoDoc bson.M
	buildInfoCmd := bson.D{{"buildInfo", 1}}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err = client.Database("admin").RunCommand(ctx, buildInfoCmd).Decode(&buildInfoDoc)
	if err != nil {
		return
	}
	if _, ok := buildInfoDoc["version"]; !ok {
		err = fmt.Errorf("get mongo version empty")
		return
	}
	version = fmt.Sprint(buildInfoDoc["version"])
	return
}
