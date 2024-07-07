package src

import (
	"encoding/json"
	. "github.com/smartystreets/goconvey/convey"
	"strconv"
	"testing"
)

func Test_data2String(t *testing.T) {
	c := new(Conn)

	Convey("json.Number", t, func() {
		data := json.Number("111")
		result, err := c.data2String(data)
		So(err, ShouldBeNil)
		So(result, ShouldEqual, "111")
	})

	Convey("string", t, func() {
		var data interface{} = "sss"
		result, err := c.data2String(data)
		So(err, ShouldBeNil)
		So(result, ShouldEqual, "sss")
	})

	Convey("[]string", t, func() {
		var data = []string{"a1", "b1"}
		result, err := c.data2String(data)
		So(err, ShouldBeNil)
		So(result, ShouldEqual, "[\"a1\",\"b1\"]")
	})

	Convey("map", t, func() {
		var data = map[string]interface{}{"a1": "a1_val", "b1": "b1_val"}
		result, err := c.data2String(data)
		So(err, ShouldBeNil)
		So(result, ShouldContainSubstring, "a1_val")
	})

	Convey("slice", t, func() {
		var data = make([]int, 0)
		data = append(data, 100)
		data = append(data, 200)
		result, err := c.data2String(data)
		So(err, ShouldBeNil)
		So(result, ShouldEqual, "[100,200]")
	})

	Convey("Float32", t, func() {
		var data = float32(9.99)
		result, err := c.data2String(data)
		So(err, ShouldBeNil)
		So(result, ShouldEqual, strconv.FormatFloat(float64(9.99), 'E', -1, 32))
	})

	Convey("Float64", t, func() {
		var data = float64(9.99)
		result, err := c.data2String(data)
		So(err, ShouldBeNil)
		So(result, ShouldEqual, strconv.FormatFloat(float64(9.99), 'E', -1, 64))
	})

	Convey("int", t, func() {
		var data int = 100
		result, err := c.data2String(data)
		So(err, ShouldBeNil)
		So(result, ShouldEqual, "100")
	})
}
