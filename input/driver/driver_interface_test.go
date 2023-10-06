package driver

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestPluginDriverInterface_GetReplicateDoDbList(t *testing.T) {
	Convey("all", t, func() {
		c := &PluginDriverInterface{}
		c.AddReplicateDoDb("mytest", "tb_test")
		c.AddReplicateDoDb("*", "*")
		c.AddReplicateDoDb("mytest", "*")
		dbMapTableList := c.GetReplicateDoDbList()
		So(len(dbMapTableList), ShouldEqual, 1)
		So(dbMapTableList["*"][0], ShouldEqual, "*")
	})

	Convey("one db all", t, func() {
		c := &PluginDriverInterface{}
		c.AddReplicateDoDb("mytest", "tb_test")
		c.AddReplicateDoDb("mytest2", "*")
		dbMapTableList := c.GetReplicateDoDbList()
		So(len(dbMapTableList), ShouldEqual, 2)
		So(dbMapTableList["mytest2"][0], ShouldEqual, "*")
		So("tb_test", ShouldBeIn, dbMapTableList["mytest"])
	})
}
