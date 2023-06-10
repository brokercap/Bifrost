package mongo

import (
	inputDriver "github.com/brokercap/Bifrost/input/driver"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMongoInput_IsSupported(t *testing.T) {
	c := &MongoInput{}
	Convey("normal", t, func() {
		So(c.IsSupported(inputDriver.SupportFull), ShouldEqual, false)
		So(c.IsSupported(inputDriver.SupportNeedMinPosition), ShouldEqual, false)
		So(c.IsSupported(inputDriver.SupportIncre), ShouldEqual, true)
	})
}
