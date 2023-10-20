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
	var lastIsNotCalcPosition bool
	for _, tInfo := range tableMap {
		lastToServerInfo, lastIsNotCalcPosition = db.GetMinPositionToServer(tInfo, lastToServerInfo, lastIsNotCalcPosition)
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

func (db *db) GetMinPositionToServer(tInfo *Table, lastToServerInfo *ToServer, lastIsNotCalcPosition bool) (lastSuccessToServerInfo *ToServer, lastSuccessIsNotCalcPosition bool) {
	tInfo.RLock()
	defer tInfo.RUnlock()
	if len(tInfo.ToServerList) == 0 {
		return nil, false
	}
	for _, toServerInfo := range tInfo.ToServerList {
		lastToServerInfo, lastIsNotCalcPosition = db.CompareToServerPosition(lastToServerInfo, toServerInfo, lastIsNotCalcPosition)
	}
	return lastToServerInfo, lastIsNotCalcPosition
}

func (db *db) CompareToServerPosition(last, current *ToServer, lastIsNotCalcPosition bool) (lastSuccessToServerInfo *ToServer, lastSuccessIsNotCalcPosition bool) {
	if current == nil || current.LastSuccessBinlog == nil || current.LastSuccessBinlog.EventID == 0 {
		return last, false
	}
	// 场景1:
	// 表t1 一个小时前同步了1000条数据之后,就再也没有产生任务数据变更,也即一个小时前 t1表的数据同步位点假设为 1000
	// 表t2 在接下来的一个小时里,产了1500条更新,即 t2 的位点,假设为 2500(因为每一条数据变更都是在原来的基础+1,所以应该算上t1的位点,累加)
	// 在这种情况下,假如只取最小位点,则永远也计算出来为 表t1 最后同步的位点 1000 为最小,则为导致位点不会被更新,同时重启导致 t2 表大量数据重复同步
	// 所以在这种情况下,应该识别出 假如 t1 表一直没有数据更新,则应该不应该在位点计算范围
	// 如果 t1表的 同步如果队列中没有数据,并且最后进入队列的位点和最后成功的位点 是一致的情况下,则认为此表同步没有产生新数据了
	// 所以这个时候应该取 大值,而不是小值
	// 为什么取大值,而不是直接返回 被对比的另一方们位点
	// 场景2:
	// 一个数据源同步了10个表,每个表的数据都是很均匀的每2秒就只产生一条数据,因为同步很快,每次这里咱计算的时候,是不是所有表都是以上面的场景中,位点不在计算范围了
	// 所有表的同步位点都不在计算范围,那是不是取出来的值,就还是 0 值的位点?合理吗? 因为总有第一次对比,对不对,而且遍历过程中所有位点都是不在计算范围里的,就应该取最大值才对
	// 参考 recovery 逻辑
	if current.QueueMsgCount == 0 && current.LastQueueBinlog != nil && current.LastQueueBinlog.EventID == current.LastSuccessBinlog.EventID {
		if last == nil || last.LastSuccessBinlog == nil {
			return current, true
		}
		if lastIsNotCalcPosition {
			// 本次和上一次均是不属于 要计算位点的范围,取大值
			// 如果 last 更大,所以返回的也应该是 上一次的位点,以及lastSuccessIsNotCalcPosition = true
			return db.CompareToServerPositionAndReturnGreater(last, current), true
		} else {
			// 本次是不需要计算范围,假如上一次 是需要计算的位点,则直接返回上一次的位点,无视上一次是大是小
			return last, false
		}
	}
	if last == nil || last.LastSuccessBinlog == nil || last.LastSuccessBinlog.EventID == 0 {
		return current, false
	}
	// 假如上一次对比的位点,是属于 不需要在计算范围内的位点,则这次取大值
	if lastIsNotCalcPosition {
		if last.LastSuccessBinlog.EventID < current.LastSuccessBinlog.EventID {
			return current, false
		} else {
			// 假如取大值,还是上一次的的值更大,则直接返回上一次的位点,并且 lastSuccessIsNotCalcPosition = true
			// 毕竟上一次 lastIsNotCalcPosition 为 true
			return last, true
		}
	}
	return db.CompareToServerPositionAndReturnLess(last, current), false
}

func (db *db) CompareToServerPositionAndReturnLess(last, current *ToServer) (lastSuccessToServerInfo *ToServer) {
	/*
		if last == nil || last.LastSuccessBinlog == nil || last.LastSuccessBinlog.EventID == 0 {
			return current
		}
		if current == nil || current.LastSuccessBinlog == nil || current.LastSuccessBinlog.EventID == 0 {
			return last
		}
	*/
	if last.LastSuccessBinlog.EventID < current.LastSuccessBinlog.EventID {
		return last
	}
	return current
}

func (db *db) CompareToServerPositionAndReturnGreater(last, current *ToServer) (lastSuccessToServerInfo *ToServer) {
	/*
		if last == nil || last.LastSuccessBinlog == nil {
			return current
		}
		if current == nil || current.LastSuccessBinlog == nil {
			return last
		}
	*/
	if last.LastSuccessBinlog.EventID >= current.LastSuccessBinlog.EventID {
		return last
	}
	return current
}
