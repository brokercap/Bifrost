package server

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey"
	. "github.com/smartystreets/goconvey/convey"

	"github.com/brokercap/Bifrost/config"
	inputDriver "github.com/brokercap/Bifrost/input/driver"
)

func TestDb_CompareToServerPositionAndReturnLess(t *testing.T) {
	db := &db{}
	Convey("两者都为nil", t, func() {
		var last *ToServer
		var current *ToServer
		var result *ToServer = nil
		result = db.CompareToServerPositionAndReturnLess(last, current)
		So(result, ShouldEqual, nil)
	})

	Convey("last不为Nil，但LastSuccessBinlog=nil，current有值", t, func() {
		var last *ToServer = &ToServer{LastSuccessBinlog: nil}
		var current *ToServer = &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 100}}
		var result *ToServer
		result = db.CompareToServerPositionAndReturnLess(last, current)
		So(result, ShouldEqual, current)
	})

	Convey("last不为Nil，EventID=0，current有值", t, func() {
		var last *ToServer = &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 0}}
		var current *ToServer = &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 100}}
		var result *ToServer
		result = db.CompareToServerPositionAndReturnLess(last, current)
		So(result, ShouldEqual, current)
	})

	Convey("last不为nil,current=nil", t, func() {

		var last *ToServer = &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 1}}
		var current *ToServer = nil
		var result *ToServer
		result = db.CompareToServerPositionAndReturnLess(last, current)
		So(result, ShouldEqual, last)
	})

	Convey("last不为Nil，current LastSuccessBinlog=nil ", t, func() {
		var last *ToServer = &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 1}}
		var current *ToServer = &ToServer{}
		var result *ToServer
		result = db.CompareToServerPositionAndReturnLess(last, current)
		So(result, ShouldEqual, last)
	})

	Convey("last不为Nil，current EventID=0 ", t, func() {
		var last *ToServer = &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 1}}
		var current *ToServer = &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 0}}
		var result *ToServer
		result = db.CompareToServerPositionAndReturnLess(last, current)
		So(result, ShouldEqual, last)
	})

	Convey("last current均正常值 ", t, func() {
		var last *ToServer = &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 1}}
		var current *ToServer = &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 100}}
		var result *ToServer
		result = db.CompareToServerPositionAndReturnLess(last, current)
		So(result, ShouldEqual, last)
	})
}

func TestDb_CalcMinPosition(t *testing.T) {
	dbObj := &db{}
	patches := gomonkey.ApplyMethod(reflect.TypeOf(dbObj), "CompareToServerPositionAndReturnLess", func(dbObj *db, last, current *ToServer) *ToServer {
		return nil
	})
	defer patches.Reset()
	Convey("计算最小位点", t, func() {
		result := dbObj.CalcMinPosition()
		So(result, ShouldEqual, nil)
	})
}

func TestDb_CronCalcMinPosition(t *testing.T) {
	Convey("不需要定时计算位点的Input", t, func() {
		dbObj := &db{}
		dbObj.inputDriverObj = inputDriver.NewPluginDriverInterface()
		patches := gomonkey.ApplyMethod(reflect.TypeOf(dbObj.inputDriverObj), "IsSupported", func(inputDriverObj *inputDriver.PluginDriverInterface, supportType inputDriver.SupportType) bool {
			return false
		})
		defer patches.Reset()
		var overCh = make(chan error, 1)
		go func() {
			<-time.After(2 * time.Second)
			overCh <- fmt.Errorf("failed")
		}()
		go func() {
			dbObj.CronCalcMinPosition()
			overCh <- nil
		}()
		err := <-overCh
		So(err, ShouldEqual, nil)
	})

	Convey("正常运行", t, func() {
		testCtx, testCancleFunc := context.WithCancel(context.Background())
		dbObj := &db{
			statusCtx: struct {
				ctx       context.Context
				cancelFun context.CancelFunc
			}{ctx: testCtx},
		}
		dbObj.inputDriverObj = inputDriver.NewPluginDriverInterface()
		patches := gomonkey.ApplyMethod(reflect.TypeOf(dbObj.inputDriverObj), "DoneMinPosition", func(inputDriverObj *inputDriver.PluginDriverInterface, p *inputDriver.PluginPosition) error {
			testCancleFunc()
			return nil
		})
		// input设置需要定时计算最小位点
		patches.ApplyMethod(reflect.TypeOf(dbObj.inputDriverObj), "IsSupported", func(inputDriverObj *inputDriver.PluginDriverInterface, supportType inputDriver.SupportType) bool {
			return true
		})
		// mock 最小计算结果
		patches.ApplyMethod(reflect.TypeOf(dbObj), "CalcMinPosition", func(dbObj *db) *inputDriver.PluginPosition {
			return &inputDriver.PluginPosition{}
		})
		// 设置10ms计算一次
		config.CronCalcMinPositionTimeout = 10
		//patches.ApplyGlobalVar(&config.CronCalcMinPositionTimeout,10)
		defer patches.Reset()
		go dbObj.CronCalcMinPosition()
		select {
		case <-time.After(2 * time.Second):
			t.Errorf("CronCalcMinPosition failed")
			return
		case <-testCtx.Done():
			t.Log("success")
		}
	})
}
