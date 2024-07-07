//go:build integration
// +build integration

package mongo

import (
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

var mongoUri = "mongodb://127.0.0.1:27017"

func TestMongoInput_GetCollectionDataList_Integration(t *testing.T) {
	Convey("normal", t, func() {
		client, err := CreateMongoClient(mongoUri, nil)
		So(err, ShouldBeNil)
		collection := client.Database("mytest").Collection("tb_1")
		collection.Drop(nil)
		perBatchLimit := 5
		var data []interface{}
		for i := 0; i <= perBatchLimit; i++ {
			m := map[string]interface{}{
				"i":    i,
				"name": fmt.Sprintf("name_%d", i),
			}
			data = append(data, m)
		}
		collection.InsertMany(nil, data)
		c := new(MongoInput)
		batchResult, err := c.GetCollectionDataList(nil, collection, nil, perBatchLimit)
		So(err, ShouldBeNil)
		So(len(batchResult), ShouldEqual, perBatchLimit)

		for _, v := range batchResult {
			t.Log(v)
		}
		minId := batchResult[len(batchResult)-1]["_id"]
		batchResult2, err2 := c.GetCollectionDataList(nil, collection, minId, perBatchLimit)
		So(err2, ShouldBeNil)
		So(len(batchResult2), ShouldEqual, 1)
	})
}
