package server

import (
	"github.com/brokercap/Bifrost/config"
	inputDriver "github.com/brokercap/Bifrost/input/driver"
	"log"
	"time"
)

// 定时计算最小位点
func (db *db) CronCalcMinPosition() (p *inputDriver.PluginPosition) {
	// 假如不需要定时计算全量，则直接退出
	if !db.inputDriverObj.IsSupported(inputDriver.SupportNeedMinPosition) {
		return
	}
	log.Println(db.Name, " CronCalcMinPosition start")
	defer func() {
		log.Println(db.Name, " CronCalcMinPosition end")
	}()
	// 设置3500ms，是为了和 3秒的进行保存位点的任务进行错开
	timeDuration := time.Duration(config.CronCalcMinPositionTimeout) * time.Millisecond
	timer := time.NewTimer(timeDuration)
	for {
		timer.Reset(timeDuration)
		select {
		case <-timer.C:
			p := db.CalcMinPosition()
			func() {
				db.RLock()
				defer db.RUnlock()
				if db.inputDriverObj != nil {
					db.inputDriverObj.DoneMinPosition(p)
				}
			}()
			break
		case <-db.statusCtx.ctx.Done():
			return
		}
	}
}

func (db *db) CalcMinPosition() (p *inputDriver.PluginPosition) {
	db.RLock()
	tableMap := db.tableMap
	if len(tableMap) == 0 {
		return
	}
	db.RUnlock()

	var lastToServerInfo *ToServer
	for _, tInfo := range tableMap {
		lastToServerInfo = db.GetMinPositionToServer(tInfo)
	}
	if lastToServerInfo == nil {
		return
	}
	lastToServerInfo.RLock()
	defer lastToServerInfo.RUnlock()
	p = &inputDriver.PluginPosition{
		GTID:           lastToServerInfo.LastSuccessBinlog.GTID,
		BinlogFileName: "",
		BinlogPostion:  lastToServerInfo.LastSuccessBinlog.BinlogPosition,
		Timestamp:      lastToServerInfo.LastSuccessBinlog.Timestamp,
		EventID:        lastToServerInfo.LastSuccessBinlog.EventID,
	}
	return
}

func (db *db) GetMinPositionToServer(tInfo *Table) (lastToServerInfo *ToServer) {
	tInfo.RLock()
	defer tInfo.RUnlock()
	if len(tInfo.ToServerList) == 0 {
		return nil
	}
	for _, toServerInfo := range tInfo.ToServerList {
		lastToServerInfo = db.CompareToServerPositionAndReturnLess(lastToServerInfo, toServerInfo)
	}
	return lastToServerInfo
}

func (db *db) CompareToServerPositionAndReturnLess(last, current *ToServer) *ToServer {
	if last == nil || last.LastSuccessBinlog == nil || last.LastSuccessBinlog.EventID == 0 {
		return current
	}
	if current == nil || current.LastSuccessBinlog == nil || current.LastSuccessBinlog.EventID == 0 {
		return last
	}
	if last.LastSuccessBinlog.EventID <= current.LastSuccessBinlog.EventID {
		return last
	}
	return current
}
