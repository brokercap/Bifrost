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
		lastToServerInfo = db.GetMinPositionToServer(tInfo, lastToServerInfo)
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

func (db *db) GetMinPositionToServer(tInfo *Table, lastToServerInfo *ToServer) (lastSuccessToServerInfo *ToServer) {
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
	if current == nil || current.LastSuccessBinlog == nil || current.LastSuccessBinlog.EventID == 0 {
		return last
	}
	// 场景
	// 表t1 一个小时前同步了1000条数据之后,就再也没有产生任务数据变更,也即一个小时前 t1表的数据同步位点假设为 1000
	// 表t2 在接下来的一个小时里,产了1500条更新,即 t2 的位点,假设为 2500(因为每一条数据变更都是在原来的基础+1,所以应该算上t1的位点,累加)
	// 在这种情况下,假如只取最小位点,则永远也计算出来为 表t1 最后同步的位点 1000 为最小,则为导致位点不会被更新,同时重启导致 t2 表大量数据重复同步
	// 所以在这种情况下,应该识别出 假如 t1 表一直没有数据更新,则应该不应该在位点计算范围
	// 所以如果 t1表的 同步如果队列中没有数据,并且最后进入队列的位点和最后成功的位点 是一致的情况下,则认为此表同步没有产生新数据了
	// 参考 recovery 逻辑是一样的
	if current.QueueMsgCount == 0 && current.LastQueueBinlog != nil && current.LastQueueBinlog.EventID == current.LastSuccessBinlog.EventID {
		return last
	}
	if last == nil || last.LastSuccessBinlog == nil || last.LastSuccessBinlog.EventID == 0 {
		return current
	}
	if last.LastSuccessBinlog.EventID <= current.LastSuccessBinlog.EventID {
		return last
	}
	return current
}
