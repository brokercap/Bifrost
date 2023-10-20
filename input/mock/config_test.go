package mock

import (
	"github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestNewConfig(t *testing.T) {
	convey.Convey("normal", t, func() {
		uri := "PerformanceTableDataCount=50000&PerformanceTableRowsEventBatchInterval=600&PerformanceTableRowsEventBatchSize=50000&LongStringLen=36000&IsAllInsertSameData=true"
		configMap := ParseDSN(uri)
		c := NewConfig(configMap)
		convey.So(c, convey.ShouldNotBeNil)
		convey.So(c.PerformanceTableDataCount, convey.ShouldEqual, 50000)
		convey.So(c.PerformanceTableRowsEventBatchInterval, convey.ShouldEqual, 600)
		convey.So(c.PerformanceTableRowsEventBatchSize, convey.ShouldEqual, 50000)
		convey.So(c.LongStringLen, convey.ShouldEqual, 36000)
		convey.So(c.IsAllInsertSameData, convey.ShouldEqual, true)
	})
}
