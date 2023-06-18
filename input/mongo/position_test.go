package mongo

import (
	"fmt"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMongoInput_OpLogPosition2GTID(t *testing.T) {
	c := &MongoInput{}
	Convey("nil", t, func() {
		result := c.OpLogPosition2GTID(nil)
		So(result, ShouldEqual, "")
	})

	Convey("normal", t, func() {
		p := &primitive.Timestamp{100, 1}
		result := c.OpLogPosition2GTID(p)
		So(result, ShouldEqual, fmt.Sprintf("{\"T\":%d,\"I\":%d}", p.T, p.I))
	})
}

func TestMongoInput_GTID2OpLogPosition(t *testing.T) {
	c := &MongoInput{}
	Convey("nil", t, func() {
		result := c.GTID2OpLogPosition("")
		So(result, ShouldBeNil)
	})

	Convey("normal", t, func() {
		str := fmt.Sprintf("{\"T\":%d,\"I\":%d}", 100, 1)
		result := c.GTID2OpLogPosition(str)
		So(result, ShouldNotBeNil)
		So(result.T, ShouldEqual, 100)
		So(result.I, ShouldEqual, 1)
	})

	Convey("json error", t, func() {
		str := "sssssssssss"
		result := c.GTID2OpLogPosition(str)
		So(result, ShouldBeNil)
	})
}
