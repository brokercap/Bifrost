package mongo

import (
	"context"
	"errors"
	"fmt"
	"github.com/agiledragon/gomonkey/v2"
	inputDriver "github.com/brokercap/Bifrost/input/driver"
	outputDriver "github.com/brokercap/Bifrost/plugin/driver"
	. "github.com/smartystreets/goconvey/convey"
	"go.mongodb.org/mongo-driver/mongo"
	"reflect"
	"testing"
)

func TestMongoInput_BatchStart(t *testing.T) {
	Convey("GetBatchTableList error", t, func() {
		c := new(MongoInput)
		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "GetBatchTableList", func(c *MongoInput) (dbTableList map[string][]string, err error) {
			return nil, errors.New("GetBatchTableList error")
		})
		defer patches.Reset()
		err := c.BatchStart()
		So(err.Error(), ShouldEqual, "GetBatchTableList error")
	})

	Convey("CreateMongoClient error", t, func() {
		c := new(MongoInput)
		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "GetBatchTableList", func(c *MongoInput) (dbTableList map[string][]string, err error) {
			dbTableList = make(map[string][]string, 0)
			dbTableList["mytest"] = make([]string, 0)
			dbTableList["mytest"] = append(dbTableList["mytest"], "tb_1")
			return
		})
		patches.ApplyFunc(CreateMongoClient, func(uri string, ctx context.Context) (*mongo.Client, error) {
			return &mongo.Client{}, fmt.Errorf("mock error")
		})
		defer patches.Reset()
		err := c.BatchStart()
		So(err.Error(), ShouldEqual, "mock error")
	})

	Convey("TableBatchStart error", t, func() {
		c := new(MongoInput)
		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "GetBatchTableList", func(c *MongoInput) (dbTableList map[string][]string, err error) {
			dbTableList = make(map[string][]string, 0)
			dbTableList["mytest"] = make([]string, 0)
			dbTableList["mytest"] = append(dbTableList["mytest"], "tb_1")
			return
		})
		patches.ApplyFunc(CreateMongoClient, func(uri string, ctx context.Context) (*mongo.Client, error) {
			return &mongo.Client{}, nil
		})
		patches.ApplyMethod(reflect.TypeOf(c), "TableBatchStart", func(c *MongoInput, collection *mongo.Collection, perBatchLimit int) error {
			return errors.New("TableBatchStart error")
		})
		defer patches.Reset()
		err := c.BatchStart()
		So(err.Error(), ShouldEqual, "TableBatchStart error")
	})

	Convey("normal", t, func() {
		c := new(MongoInput)
		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "GetBatchTableList", func(c *MongoInput) (dbTableList map[string][]string, err error) {
			dbTableList = make(map[string][]string, 0)
			dbTableList["mytest"] = make([]string, 0)
			dbTableList["mytest"] = append(dbTableList["mytest"], "tb_1")
			return
		})
		patches.ApplyFunc(CreateMongoClient, func(uri string, ctx context.Context) (*mongo.Client, error) {
			return &mongo.Client{}, nil
		})
		patches.ApplyMethod(reflect.TypeOf(c), "TableBatchStart", func(c *MongoInput, collection *mongo.Collection, perBatchLimit int) error {
			return nil
		})
		defer patches.Reset()
		err := c.BatchStart()
		So(err, ShouldBeNil)
	})
}

func TestMongoInput_GetBatchTableList(t *testing.T) {
	Convey("* table,get GetSchemaList error", t, func() {
		c := new(MongoInput)
		c.AddReplicateDoDb0("*", "*")
		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "GetSchemaList", func(c *MongoInput) (data []string, err error) {
			err = errors.New("GetSchemaList error")
			return
		})
		defer patches.Reset()
		_, err := c.GetBatchTableList()
		So(err.Error(), ShouldEqual, "GetSchemaList error")
	})

	Convey("* table,get GetSchemaTableList error", t, func() {
		c := new(MongoInput)
		c.AddReplicateDoDb0("*", "*")
		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "GetSchemaList", func(c *MongoInput) (data []string, err error) {
			return []string{"mytest"}, nil
		})
		patches.ApplyMethod(reflect.TypeOf(c), "GetSchemaTableList", func(c *MongoInput, schema string) (tableList []inputDriver.TableList, err error) {
			err = errors.New("GetSchemaTableList error")
			return
		})
		defer patches.Reset()
		_, err := c.GetBatchTableList()
		So(err.Error(), ShouldEqual, "GetSchemaTableList error")
	})

	Convey("* table,normal", t, func() {
		c := new(MongoInput)
		c.AddReplicateDoDb0("*", "*")
		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "GetSchemaList", func(c *MongoInput) (data []string, err error) {
			return []string{"mytest"}, nil
		})
		patches.ApplyMethod(reflect.TypeOf(c), "GetSchemaTableList", func(c *MongoInput, schema string) (tableList []inputDriver.TableList, err error) {
			tableList = append(tableList, inputDriver.TableList{
				TableName: "tb_1",
			})
			tableList = append(tableList, inputDriver.TableList{
				TableName: "tb_2",
			})
			tableList = append(tableList, inputDriver.TableList{
				TableName: "tb_3",
			})
			return
		})
		defer patches.Reset()
		dbTableList, err := c.GetBatchTableList()
		So(err, ShouldBeNil)
		So(len(dbTableList), ShouldEqual, 1)
		So(len(dbTableList["mytest"]), ShouldEqual, 3)
	})

	Convey("no *", t, func() {
		c := new(MongoInput)
		c.AddReplicateDoDb0("mytest", "tb_1")
		c.AddReplicateDoDb0("mytest", "tb_2")
		c.AddReplicateDoDb0("mytest_2", "tb_1")
		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "GetSchemaTableList", func(c *MongoInput, schema string) (tableList []inputDriver.TableList, err error) {
			tableList = append(tableList, inputDriver.TableList{
				TableName: "tb_1",
			})
			tableList = append(tableList, inputDriver.TableList{
				TableName: "tb_2",
			})
			tableList = append(tableList, inputDriver.TableList{
				TableName: "tb_3",
			})
			return
		})
		defer patches.Reset()
		dbTableList, err := c.GetBatchTableList()
		So(err, ShouldBeNil)
		So(len(dbTableList), ShouldEqual, 2)
		So(len(dbTableList["mytest"]), ShouldEqual, 2)
		So(len(dbTableList["mytest_2"]), ShouldEqual, 1)
	})
}

func TestMongoInput_TableBatchStart(t *testing.T) {
	Convey("GetCollectionDataList err", t, func() {
		c := new(MongoInput)
		client := &mongo.Client{}
		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "GetCollectionDataList", func(c *MongoInput, ctx context.Context, collection *mongo.Collection, minId interface{}, perBatchLimit int) (batchResult []map[string]interface{}, err error) {
			err = errors.New("GetCollectionDataList error")
			return
		})
		defer patches.Reset()
		collection := c.GetCollection(client, "mytest", "tb_1")
		err := c.TableBatchStart(collection, 1000)
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, "GetCollectionDataList error")
	})

	Convey("perBatchLimit = 5,second len = 1", t, func() {
		perBatchLimit := 5
		c := new(MongoInput)
		client := &mongo.Client{}
		var GetCollectionDataListMockI = 0
		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "GetCollectionDataList", func(c *MongoInput, ctx context.Context, collection *mongo.Collection, minId interface{}, perBatchLimit int) (batchResult []map[string]interface{}, err error) {
			switch GetCollectionDataListMockI {
			case 0:
				for i := 0; i < perBatchLimit; i++ {
					m := map[string]interface{}{
						"_id":                        i,
						"GetCollectionDataListMockI": GetCollectionDataListMockI,
						"k1":                         "v1",
						"k2":                         2,
						"k3":                         3.99,
					}
					batchResult = append(batchResult, m)
				}
			default:
				m := map[string]interface{}{
					"_id":                        1000000,
					"GetCollectionDataListMockI": GetCollectionDataListMockI,
					"k1":                         "v1",
					"k2":                         2,
					"k3":                         3.99,
				}
				batchResult = append(batchResult, m)
			}
			GetCollectionDataListMockI++
			return
		})
		defer patches.Reset()

		var callbackDataList []*outputDriver.PluginDataType
		var callbackFun = func(data *outputDriver.PluginDataType) {
			callbackDataList = append(callbackDataList, data)
		}
		c.callback = callbackFun
		collection := c.GetCollection(client, "mytest", "tb_1")
		err := c.TableBatchStart(collection, perBatchLimit)
		So(err, ShouldBeNil)
		So(len(callbackDataList), ShouldEqual, 6)
	})

	Convey("perBatchLimit = 5,second len = 0", t, func() {
		perBatchLimit := 5
		c := new(MongoInput)
		client := &mongo.Client{}
		var GetCollectionDataListMockI = 0
		patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "GetCollectionDataList", func(c *MongoInput, ctx context.Context, collection *mongo.Collection, minId interface{}, perBatchLimit int) (batchResult []map[string]interface{}, err error) {
			switch GetCollectionDataListMockI {
			case 0:
				for i := 0; i < perBatchLimit; i++ {
					m := map[string]interface{}{
						"_id":                        i,
						"GetCollectionDataListMockI": GetCollectionDataListMockI,
						"k1":                         "v1",
						"k2":                         2,
						"k3":                         3.99,
					}
					batchResult = append(batchResult, m)
				}
			default:
				return
			}
			GetCollectionDataListMockI++
			return
		})
		defer patches.Reset()

		var callbackDataList []*outputDriver.PluginDataType
		var callbackFun = func(data *outputDriver.PluginDataType) {
			callbackDataList = append(callbackDataList, data)
		}
		c.callback = callbackFun
		collection := c.GetCollection(client, "mytest", "tb_1")
		err := c.TableBatchStart(collection, perBatchLimit)
		So(err, ShouldBeNil)
		So(len(callbackDataList), ShouldEqual, perBatchLimit)
	})
}

func TestMongoInput_BatchResult2RowEvent(t *testing.T) {
	Convey("normal", t, func() {
		schemaName := "mytest"
		tableName := "tb_1"
		m := map[string]interface{}{
			"_id": 100,
			"key": "val",
		}
		c := new(MongoInput)
		eventData := c.BatchResult2RowEvent(schemaName, tableName, m)
		So(eventData.EventType, ShouldEqual, "insert")
		So(len(eventData.Pri), ShouldEqual, 1)
		So(eventData.Pri[0], ShouldEqual, "_id")
		So(eventData.ColumnMapping, ShouldNotBeNil)
	})
}
