package driver

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestPluginDataType_ToCanalJsonObject(t *testing.T) {
	i := 1
	if i&1 != 0 {
		t.Log(i, " 奇数")
	}
	i = 0
	if i&1 == 0 {
		t.Log(0, " 偶数")
	}
	dataType := "Nullable(int(11))"

	dataType = dataType[9 : len(dataType)-1]
	t.Log(dataType)

	Convey("update event", t, func() {
		c, err := NewPluginDataCanal([]byte(canal_update_event_data))
		So(err, ShouldEqual, nil)
		bifrostEventData := c.ToBifrostOutputPluginData()
		So(bifrostEventData.EventType, ShouldEqual, "update")
		So(bifrostEventData.Pri, ShouldResemble, c.PkNames)
		So(bifrostEventData.Rows[1], ShouldResemble, c.Data[0])
		So(bifrostEventData.Rows[0], ShouldResemble, c.Old[0])
		So(bifrostEventData.SchemaName, ShouldNotEqual, "")
		So(bifrostEventData.TableName, ShouldNotEqual, "")
		So(bifrostEventData.SchemaName, ShouldEqual, c.Database)
		So(bifrostEventData.TableName, ShouldNotEqual, c.Table)
	})
}
