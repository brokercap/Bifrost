package server

import (
	"errors"
	"github.com/agiledragon/gomonkey"
	"github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
	"time"
)

func Test_setServerStartTime(t *testing.T) {
	convey.Convey("set zero time,get time by config file return no-zero", t, func() {
		patch := gomonkey.ApplyFunc(GetServerStartTimeByConfigFile, func() time.Time {
			return time.Now()
		})
		defer patch.Reset()
		var t time.Time
		setServerStartTime(t)
		convey.So(GetServerStartTime().IsZero(), convey.ShouldBeFalse)
	})

	convey.Convey("set zero time,get time by config file return zero", t, func() {
		patch := gomonkey.ApplyFunc(GetServerStartTimeByConfigFile, func() time.Time {
			var t time.Time
			return t
		})
		defer patch.Reset()
		var t time.Time
		setServerStartTime(t)
		convey.So(GetServerStartTime().IsZero(), convey.ShouldBeTrue)
	})
}

func Test_setServerStartTime_AfterTime(t *testing.T) {
	convey.Convey("set before time", t, func() {
		var nowTime = time.Now()
		var beforeTime = nowTime.AddDate(0, 0, -1)
		setServerStartTime(nowTime)
		setServerStartTime(beforeTime)
		convey.So(GetServerStartTime(), convey.ShouldEqual, beforeTime)
	})
}

func Test_setServerStartTime_BeforeTime(t *testing.T) {
	convey.Convey("set before time", t, func() {
		var nowTime = time.Now()
		var beforeTime = nowTime.AddDate(0, 0, -1)
		setServerStartTime(beforeTime)
		setServerStartTime(nowTime)
		convey.So(GetServerStartTime(), convey.ShouldEqual, beforeTime)
	})
}

func Test_GetServerStartTimeByConfigFile(t *testing.T) {
	convey.Convey("get config file error", t, func() {
		patch := gomonkey.ApplyFunc(os.Stat, func(file string) (os.FileInfo, error) {
			return nil, errors.New("test error")
		})
		defer patch.Reset()
		modTime := GetServerStartTimeByConfigFile()
		convey.So(time.Now().Sub(modTime).Seconds(), convey.ShouldBeBetween, 0, 2)
	})
}
