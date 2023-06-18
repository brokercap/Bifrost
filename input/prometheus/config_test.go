package prometheus

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestParseDSN(t *testing.T) {
	Convey("dsn == \"\"", t, func() {
		m := ParseDSN("")
		So(len(m), ShouldEqual, 0)
	})
	Convey("dsn have param \"\"", t, func() {
		dsn := "http://127.0.0.1:9090/api/v1/query?query=target&input.time.interval=600&input.http.timeout=15000&start=1686317146&end=1686320745"
		m := ParseDSN(dsn)
		So(m["url"], ShouldEqual, "http://127.0.0.1:9090/api/v1/query?query=target")
		So(m["input.time.interval"], ShouldEqual, "600")
		So(m["input.http.timeout"], ShouldEqual, "15000")
		So(m["start"], ShouldEqual, "1686317146")
		So(m["end"], ShouldEqual, "1686320745")
	})

	Convey("dsn only query \"\"", t, func() {
		dsn := "http://127.0.0.1:9090/api/v1/query?query=target"
		m := ParseDSN(dsn)
		So(m["url"], ShouldEqual, "http://127.0.0.1:9090/api/v1/query?query=target")
		So(len(m), ShouldEqual, 2)
	})

	Convey("dsn param error \"\"", t, func() {
		dsn := "http://127.0.0.1:9090/api/v1/query?query=target&end=1686320745=1"
		m := ParseDSN(dsn)
		So(m["url"], ShouldEqual, "http://127.0.0.1:9090/api/v1/query?query=target")
		So(len(m), ShouldEqual, 3)
	})

}

func TestGetConfig(t *testing.T) {
	Convey("param is empty \"\"", t, func() {
		dsn := "http://127.0.0.1:9090/api/v1/query?query=target&input.time.interval=600&input.http.timeout=15000&start=1686317146&end=1686320745"
		m := ParseDSN(dsn)
		c, err := getConfig(m)
		So(err, ShouldBeNil)
		So(c.Start, ShouldEqual, 1686317146)
		So(c.End, ShouldEqual, 1686320745)
		So(c.HttpTimeoutParam, ShouldEqual, 15000)
		So(c.HttpTimeout, ShouldEqual, 15*time.Second)
		So(c.TimeInterval, ShouldEqual, 600)
	})

	Convey("param is default \"\"", t, func() {
		dsn := "http://127.0.0.1:9090/api/v1/query?query=target"
		m := ParseDSN(dsn)
		c, err := getConfig(m)
		So(err, ShouldBeNil)
		So(c.Start, ShouldEqual, 0)
		So(c.End, ShouldEqual, 0)
		So(c.HttpTimeout, ShouldEqual, 30*time.Second)
		So(c.TimeInterval, ShouldEqual, 300)
	})
}
