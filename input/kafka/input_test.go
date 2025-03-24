package kafka

import (
	"fmt"
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	inputDriver "github.com/brokercap/Bifrost/input/driver"
)

func TestInputKafka_GetCosumeGroupId(t *testing.T) {
	c := NewInputKafka()
	c.inputInfo = inputDriver.InputInfo{DbName: "id_中—_-1"}

	Convey("自动生成消费组ID", t, func() {
		groupId := c.GetCosumeGroupId("")
		So(groupId, ShouldEqual, fmt.Sprintf("%s%s", defaultKafkaGroupIdPrefix, "id__1"))
	})

	Convey("指定消费者组ID", t, func() {
		groupId := c.GetCosumeGroupId("test_1")
		So(groupId, ShouldEqual, "test_1")
	})
}
