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
	Convey("last current均正常值 ", t, func() {
		var last *ToServer = &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 1}}
		var current *ToServer = &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 100}}
		var result *ToServer
		result = db.CompareToServerPositionAndReturnLess(last, current)
		So(result, ShouldEqual, last)
	})
}

func TestDb_CompareToServerPositionAndReturnGreater(t *testing.T) {
	db := &db{}
	Convey("last current均正常值 ", t, func() {
		var last *ToServer = &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 1}}
		var current *ToServer = &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 100}}
		var result *ToServer
		result = db.CompareToServerPositionAndReturnGreater(last, current)
		So(result, ShouldEqual, current)
	})
}

func TestDb_CompareToServerPosition(t *testing.T) {
	db := &db{}
	Convey("last ,current all is nil ", t, func() {
		var result *ToServer
		result, _ = db.CompareToServerPosition(nil, nil, false)
		So(result, ShouldBeNil)
	})
	Convey("current.LastSuccessBinlog is nil or eventId == 0 ", t, func() {
		{
			var last *ToServer = &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 1}}
			var current *ToServer = &ToServer{LastSuccessBinlog: nil}
			var result *ToServer
			result, _ = db.CompareToServerPosition(last, current, false)
			So(result, ShouldEqual, last)
		}
		{
			var last *ToServer = &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 1}}
			var current *ToServer = &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 0}}
			var result *ToServer
			result, _ = db.CompareToServerPosition(last, current, false)
			So(result, ShouldEqual, last)
		}
	})
	Convey("current position should not be calc,last is nil ", t, func() {
		var last *ToServer = nil
		var current *ToServer = &ToServer{
			LastSuccessBinlog: &PositionStruct{EventID: 1},
			LastQueueBinlog:   &PositionStruct{EventID: 1},
		}
		var result *ToServer
		var lastIsNotCalcPosition bool
		result, lastIsNotCalcPosition = db.CompareToServerPosition(last, current, false)
		So(result, ShouldEqual, current)
		So(lastIsNotCalcPosition, ShouldEqual, true)
	})
	Convey("current last position should not be calc,lastIsNotCalcPosition == true ,return greater  ", t, func() {
		{
			var last *ToServer = &ToServer{
				LastSuccessBinlog: &PositionStruct{EventID: 100},
				LastQueueBinlog:   &PositionStruct{EventID: 100},
			}
			var current *ToServer = &ToServer{
				LastSuccessBinlog: &PositionStruct{EventID: 1},
				LastQueueBinlog:   &PositionStruct{EventID: 1},
			}
			var result *ToServer
			var lastIsNotCalcPosition bool
			result, lastIsNotCalcPosition = db.CompareToServerPosition(last, current, true)
			So(result, ShouldEqual, last)
			So(lastIsNotCalcPosition, ShouldEqual, true)
		}

		{
			var last2 *ToServer = &ToServer{
				LastSuccessBinlog: &PositionStruct{EventID: 1},
				LastQueueBinlog:   &PositionStruct{EventID: 1},
			}
			var current2 *ToServer = &ToServer{
				LastSuccessBinlog: &PositionStruct{EventID: 100},
				LastQueueBinlog:   &PositionStruct{EventID: 100},
			}
			var result2 *ToServer
			var lastIsNotCalcPosition bool
			result2, lastIsNotCalcPosition = db.CompareToServerPosition(last2, current2, true)
			So(result2, ShouldEqual, current2)
			So(lastIsNotCalcPosition, ShouldEqual, true)
		}
	})

	Convey("current position should not be calc, lastIsNotCalcPosition == false ", t, func() {
		{
			var last *ToServer = &ToServer{
				LastSuccessBinlog: &PositionStruct{EventID: 100},
				LastQueueBinlog:   &PositionStruct{EventID: 100},
			}
			var current *ToServer = &ToServer{
				LastSuccessBinlog: &PositionStruct{EventID: 1},
				LastQueueBinlog:   &PositionStruct{EventID: 1},
			}
			var result *ToServer
			var lastIsNotCalcPosition bool
			result, lastIsNotCalcPosition = db.CompareToServerPosition(last, current, false)
			So(result, ShouldEqual, last)
			So(lastIsNotCalcPosition, ShouldEqual, false)
		}
		{
			var last2 *ToServer = &ToServer{
				LastSuccessBinlog: &PositionStruct{EventID: 1},
				LastQueueBinlog:   &PositionStruct{EventID: 1},
			}
			var current2 *ToServer = &ToServer{
				LastSuccessBinlog: &PositionStruct{EventID: 100},
				LastQueueBinlog:   &PositionStruct{EventID: 100},
			}
			var result2 *ToServer
			var lastIsNotCalcPosition bool
			result2, lastIsNotCalcPosition = db.CompareToServerPosition(last2, current2, false)
			So(result2, ShouldEqual, last2)
			So(lastIsNotCalcPosition, ShouldEqual, false)
		}
	})

	Convey("last or LastSuccessBinlog is nil or eventId == 0 ,current is not nil and normal", t, func() {
		var current *ToServer = &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 1}}
		{
			var result *ToServer
			result, _ = db.CompareToServerPosition(nil, current, false)
			So(result, ShouldEqual, current)
		}
		{
			var last *ToServer = &ToServer{LastSuccessBinlog: nil}
			var result *ToServer
			result, _ = db.CompareToServerPosition(last, current, false)
			So(result, ShouldEqual, current)
		}
		{
			var last *ToServer = &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 0}}
			var result *ToServer
			result, _ = db.CompareToServerPosition(last, current, false)
			So(result, ShouldEqual, current)
		}
	})

	Convey("lastIsNotCalcPosition == true ,current IsNotCalcPosition == false, last is less,return current", t, func() {
		{
			var last *ToServer = &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 1}, LastQueueBinlog: &PositionStruct{EventID: 1}}
			var current *ToServer = &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 100}}
			var result *ToServer
			var lastIsNotCalcPosition bool
			result, lastIsNotCalcPosition = db.CompareToServerPosition(last, current, true)
			So(result, ShouldEqual, current)
			So(lastIsNotCalcPosition, ShouldEqual, false)
		}
	})

	Convey("lastIsNotCalcPosition == true ,current IsNotCalcPosition == false, last is greater,return last", t, func() {
		{
			var last *ToServer = &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 100}, LastQueueBinlog: &PositionStruct{EventID: 100}}
			var current *ToServer = &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 1}}
			var result *ToServer
			var lastIsNotCalcPosition bool
			result, lastIsNotCalcPosition = db.CompareToServerPosition(last, current, true)
			So(result, ShouldEqual, last)
			So(lastIsNotCalcPosition, ShouldEqual, true)
		}
	})

	Convey("lastIsNotCalcPosition == false ,last ,current normal,return less", t, func() {
		{
			var last *ToServer = &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 100}}
			var current *ToServer = &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 1}}
			var result *ToServer
			var lastIsNotCalcPosition bool
			result, lastIsNotCalcPosition = db.CompareToServerPosition(last, current, false)
			So(result, ShouldEqual, current)
			So(lastIsNotCalcPosition, ShouldEqual, false)
		}

		{
			var last *ToServer = &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 100}}
			var current *ToServer = &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 1000}}
			var result *ToServer
			var lastIsNotCalcPosition bool
			result, lastIsNotCalcPosition = db.CompareToServerPosition(last, current, false)
			So(result, ShouldEqual, last)
			So(lastIsNotCalcPosition, ShouldEqual, false)
		}
	})
}

func TestDb_CalcMinPosition(t *testing.T) {
	dbObj := &db{}
	Convey("nil", t, func() {
		patches := gomonkey.ApplyMethod(reflect.TypeOf(dbObj), "CompareToServerPositionAndReturnLess", func(dbObj *db, last, current *ToServer) *ToServer {
			return nil
		})
		defer patches.Reset()
		result := dbObj.CalcMinPosition()
		So(result, ShouldEqual, nil)
	})

	Convey("计算最小位点", t, func() {
		dbObj.tableMap = make(map[string]*Table, 0)
		t1 := &Table{ToServerList: make([]*ToServer, 0)}
		t1.ToServerList = append(t1.ToServerList, &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 5}, LastQueueBinlog: &PositionStruct{EventID: 5}})
		t1.ToServerList = append(t1.ToServerList, &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 1}, LastQueueBinlog: &PositionStruct{EventID: 1}})
		t1.ToServerList = append(t1.ToServerList, &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 6}, LastQueueBinlog: &PositionStruct{EventID: 6}})
		t1.ToServerList = append(t1.ToServerList, &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 20}, LastQueueBinlog: &PositionStruct{EventID: 100}})
		t1.ToServerList = append(t1.ToServerList, &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 10}, LastQueueBinlog: &PositionStruct{EventID: 100}})

		t2 := &Table{ToServerList: make([]*ToServer, 0)}
		t2.ToServerList = append(t2.ToServerList, &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 1}, LastQueueBinlog: &PositionStruct{EventID: 1}})
		t2.ToServerList = append(t2.ToServerList, &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 100}, LastQueueBinlog: &PositionStruct{EventID: 1000}})
		t2.ToServerList = append(t2.ToServerList, &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 20}, LastQueueBinlog: &PositionStruct{EventID: 20}})

		t3 := &Table{ToServerList: make([]*ToServer, 0)}
		t3.ToServerList = append(t3.ToServerList, &ToServer{LastSuccessBinlog: &PositionStruct{EventID: 200}, LastQueueBinlog: &PositionStruct{EventID: 200}})

		dbObj.tableMap["t1"] = t1
		dbObj.tableMap["t2"] = t2
		result := dbObj.CalcMinPosition()
		So(result.EventID, ShouldEqual, 10)
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
