package mongo

import (
	"context"
	outputDriver "github.com/brokercap/Bifrost/plugin/driver"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"time"
)

func (c *MongoInput) BatchStart() (err error) {
	log.Printf("[INFO] output[%s] BatchStart starting \n", OutPutName)
	defer log.Printf("[INFO] output[%s] BatchStart end \n", OutPutName)
	dbTableList, err := c.GetBatchTableList()
	if err != nil {
		return err
	}
	client, err := CreateMongoClient(c.inputInfo.ConnectUri, nil)
	if err != nil {
		return err
	}
	defer func() {
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		client.Disconnect(ctx)
	}()
	for schemaName, tableList := range dbTableList {
		for _, tableName := range tableList {
			collection := c.GetCollection(client, schemaName, tableName)
			err = c.TableBatchStart(collection, PerBatchLimit)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *MongoInput) GetBatchTableList() (dbTableList map[string][]string, err error) {
	dbMapAndTableList := c.GetReplicateDoDbList()
	dbTableList = make(map[string][]string, 0)
	var schemaNameList []string
	if _, ok := dbMapAndTableList["*"]; ok {
		schemaNameList, err = c.GetSchemaList()
		if err != nil {
			return
		}
	} else {
		for schemaName, _ := range dbMapAndTableList {
			schemaNameList = append(schemaNameList, schemaName)
		}
	}
	for _, schemaName := range schemaNameList {
		tableList, err1 := c.GetSchemaTableList(schemaName)
		if err1 != nil {
			return nil, err1
		}
		dbTableList[schemaName] = make([]string, 0)
		for _, v := range tableList {
			if c.CheckReplicateDb(schemaName, v.TableName) {
				dbTableList[schemaName] = append(dbTableList[schemaName], v.TableName)
			}
		}
	}
	return
}

func (c *MongoInput) GetCollection(client *mongo.Client, schemaName, tableName string) *mongo.Collection {
	return client.Database(schemaName).Collection(tableName)
}

func (c *MongoInput) TableBatchStart(collection *mongo.Collection, perBatchLimit int) error {
	var schemaName = collection.Database().Name()
	var tableName = collection.Name()

	log.Printf("[INFO] output[%s] schemaName:%s tableName:%s scheTableBatchStart starting \n", OutPutName, schemaName, tableName)
	defer log.Printf("[INFO] output[%s] schemaName:%s tableName:%s scheTableBatchStart end \n", OutPutName, schemaName, tableName)

	var nextMinId interface{}
	for {
		batchResult, err := c.GetCollectionDataList(c.ctx, collection, nextMinId, perBatchLimit)
		if err != nil {
			return err
		}
		if len(batchResult) == 0 {
			break
		}
		for _, batchInfo := range batchResult {
			eventData := c.BatchResult2RowEvent(schemaName, tableName, batchInfo)
			c.callback(eventData)
		}
		if len(batchResult) < perBatchLimit {
			break
		}
		nextMinId = batchResult[len(batchResult)-1]["_id"]
	}
	return nil
}

func (c *MongoInput) GetCollectionDataList(ctx context.Context, collection *mongo.Collection, minId interface{}, perBatchLimit int) (batchResult []map[string]interface{}, err error) {
	findOptions := options.Find()
	findOptions.SetLimit(int64(perBatchLimit))
	findOptions.SetSort(bson.M{"_id": 1})
	filter := bson.D{}
	if minId != nil {
		idCond := bson.M{}
		idCond["$gt"] = minId
		filter = append(filter, bson.E{"_id", idCond})
	}
	cursor, err := collection.Find(ctx, filter, findOptions)
	if err != nil {
		return nil, err
	}
	for cursor.Next(ctx) {
		result := make(map[string]interface{})
		if err = cursor.Decode(&result); err != nil {
			return
		}
		batchResult = append(batchResult, result)
	}
	if err = cursor.Err(); err != nil {
		return
	}
	cursor.Close(ctx)
	return
}

func (c *MongoInput) BatchResult2RowEvent(schemaName, tableName string, data map[string]interface{}) (eventData *outputDriver.PluginDataType) {
	eventData = &outputDriver.PluginDataType{
		Timestamp:       uint32(time.Now().Unix()),
		EventSize:       0,
		EventType:       "insert",
		Rows:            []map[string]interface{}{data},
		SchemaName:      schemaName,
		TableName:       tableName,
		AliasSchemaName: schemaName,
		AliasTableName:  tableName,
		BinlogFileNum:   1,
		BinlogPosition:  0,
		Gtid:            c.inputInfo.GTID,
		Pri:             []string{"_id"},
		EventID:         c.getNextEventID(),
		ColumnMapping:   c.TransferDataAndColumnMapping(data),
	}
	return
}
