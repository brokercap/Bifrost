package driver

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestPluginDataType_ToCanalJsonObject(t *testing.T) {
	Convey("Bifrost转Canal", t, func() {
		c, err := NewPluginDataCanal([]byte(canal_update_event_data))
		So(err, ShouldEqual, nil)
		bifrostEventData := c.ToBifrostOutputPluginData()
		canalData, err := bifrostEventData.ToCanalJsonObject()
		So(err, ShouldEqual, nil)
		So(canalData.SqlType["id"], ShouldEqual, -5)
	})
}
