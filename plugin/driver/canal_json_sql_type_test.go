package driver

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestGetCanalSqlTypeByDataType(t *testing.T) {
	Convey("int dateType", t, func() {
		So(GetCanalSqlTypeByDataType("int"), ShouldEqual, -5)
		So(GetCanalSqlTypeByDataType("unknow"), ShouldEqual, 12)
	})
}
