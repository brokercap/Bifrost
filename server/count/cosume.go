package count

import (
	"log"
	"runtime"
	"runtime/debug"
	"time"
)

func channel_flowcount_sonsume(db string, channelId string, flowchan chan *FlowCount) {
	defer func() {
		if err := recover(); err != nil {
			delChannelChan(db + "-" + channelId)
			log.Println(db, channelId, " channel_flowcount_sonsume recover: ", err, string(debug.Stack()))
		}
	}()

	log.Println(db, channelId, "channel count start")
	defer func() {
		log.Println(db, channelId, "channel count over")
	}()
	var DoMinuteSlice bool = false
	var DoTenMinuteSlice bool = false
	var DoHourSlice bool = false
	var DoEightHourSlice bool = false
	var DoDaySlice bool = false

	var fori uint = 0
	var seliceTime int64
	nowTime := time.Now().Unix()
	seliceTime = nowTime - (nowTime % 5)
	var doDbSlice bool = true
	timer := time.NewTimer(5 * time.Second)
	var dbCountInfo = dbCountChanMap[db]
	var dbCountTableInfo *CountFlow
	var dbCountChannelInfo *CountFlow
	var ok bool
	defer timer.Stop()
	for {
		select {
		case data := <-flowchan:
			if data == nil {
				runtime.Goexit()
				break
			}
			timer.Reset(5 * time.Second)
			if data.Count < 0 {
				if data.Count == -2 {
					log.Println(db, channelId, "channel count close")
					runtime.Goexit()
					break
				}
				if data.Count == -3 {
					//count DoInit 里的协程定时，每5秒往这个chan里发送一条信息
					seliceTime = data.Time - (data.Time % 5)
					dbCountInfo.Lock()
					if dbCountInfo.doSliceTime == seliceTime {
						doDbSlice = false
					} else {
						dbCountInfo.doSliceTime = seliceTime
						doDbSlice = true
					}
					dbCountInfo.Unlock()

					fori++
					DoMinuteSlice = true
					DoTenMinuteSlice = true
					if fori%6 == 0 {
						DoHourSlice = true
					}
					if fori%60 == 0 {
						DoEightHourSlice = true
					}
					if fori%120 == 0 {
						fori = 0
						DoDaySlice = true
					}
					continue
				}
			}
			dbCountInfo.Lock()
			if dbCountTableInfo, ok = dbCountInfo.TableMap[data.TableId]; !ok {
				dbCountInfo.Unlock()
				continue
			}
			dbCountChannelInfo = dbCountInfo.ChannelMap[channelId]

			if DoMinuteSlice == true {
				DoMinuteSlice = false

				if doDbSlice == true {

					//db总计
					dbCountInfo.Flow.Minute = append(
						dbCountInfo.Flow.Minute,
						CountContent{
							Time:     seliceTime,
							Count:    dbCountInfo.Content.Count,
							ByteSize: dbCountInfo.Content.ByteSize,
						})
					dbCountInfo.Flow.Minute = dbCountInfo.Flow.Minute[1:]
				}

				//表的统计信息
				dbCountTableInfo.Minute = append(
					dbCountTableInfo.Minute,
					CountContent{
						Time:     seliceTime,
						Count:    dbCountTableInfo.Content.Count,
						ByteSize: dbCountTableInfo.Content.ByteSize,
					})

				//通道信息

				dbCountChannelInfo.Minute = append(
					dbCountChannelInfo.Minute,
					CountContent{
						Time:     seliceTime,
						Count:    dbCountChannelInfo.Content.Count,
						ByteSize: dbCountChannelInfo.Content.ByteSize,
					})
				dbCountTableInfo.Minute = dbCountTableInfo.Minute[1:]
				dbCountChannelInfo.Minute = dbCountChannelInfo.Minute[1:]
			}

			if DoTenMinuteSlice == true {
				DoTenMinuteSlice = false

				dbCountTableInfo.TenMinute = append(
					dbCountTableInfo.TenMinute,
					CountContent{
						Time:     seliceTime,
						Count:    dbCountTableInfo.Content.Count,
						ByteSize: dbCountTableInfo.Content.ByteSize,
					})

				//通道信息
				dbCountChannelInfo.TenMinute = append(
					dbCountChannelInfo.TenMinute,
					CountContent{
						Time:     seliceTime,
						Count:    dbCountChannelInfo.Content.Count,
						ByteSize: dbCountChannelInfo.Content.ByteSize,
					})

				dbCountTableInfo.TenMinute = dbCountTableInfo.TenMinute[1:]
				dbCountChannelInfo.TenMinute = dbCountChannelInfo.TenMinute[1:]

				if doDbSlice == true {

					//db总计
					dbCountInfo.Flow.TenMinute = append(
						dbCountInfo.Flow.TenMinute,
						CountContent{
							Time:     seliceTime,
							Count:    dbCountInfo.Content.Count,
							ByteSize: dbCountInfo.Content.ByteSize,
						})
					dbCountInfo.Flow.TenMinute = dbCountInfo.Flow.TenMinute[1:]
				}

			}

			//每30秒一条数据
			if DoHourSlice == true {
				DoHourSlice = false

				dbCountTableInfo.Hour = append(
					dbCountTableInfo.Hour,
					CountContent{
						Time:     seliceTime,
						Count:    dbCountTableInfo.Content.Count,
						ByteSize: dbCountTableInfo.Content.ByteSize,
					})

				//通道信息
				dbCountChannelInfo.Hour = append(
					dbCountChannelInfo.Hour,
					CountContent{
						Time:     seliceTime,
						Count:    dbCountChannelInfo.Content.Count,
						ByteSize: dbCountChannelInfo.Content.ByteSize,
					})

				dbCountTableInfo.Hour = dbCountTableInfo.Hour[1:]
				dbCountChannelInfo.Hour = dbCountChannelInfo.Hour[1:]

				if doDbSlice == true {
					//db总计
					dbCountInfo.Flow.Hour = append(
						dbCountInfo.Flow.Hour,
						CountContent{
							Time:     seliceTime,
							Count:    dbCountInfo.Content.Count,
							ByteSize: dbCountInfo.Content.ByteSize,
						})
					dbCountInfo.Flow.Hour = dbCountInfo.Flow.Hour[1:]

				}
			}
			if DoEightHourSlice == true {
				DoEightHourSlice = false
				//每5分钟一条数据

				dbCountTableInfo.EightHour = append(
					dbCountTableInfo.EightHour,
					CountContent{
						Time:     seliceTime,
						Count:    dbCountTableInfo.Content.Count,
						ByteSize: dbCountTableInfo.Content.ByteSize,
					})

				//通道信息
				dbCountChannelInfo.EightHour = append(
					dbCountChannelInfo.EightHour,
					CountContent{
						Time:     seliceTime,
						Count:    dbCountInfo.ChannelMap[channelId].Content.Count,
						ByteSize: dbCountInfo.ChannelMap[channelId].Content.ByteSize,
					})
				dbCountTableInfo.EightHour = dbCountTableInfo.EightHour[1:]
				dbCountChannelInfo.EightHour = dbCountChannelInfo.EightHour[1:]

				if doDbSlice == true {
					//db总计
					dbCountInfo.Flow.EightHour = append(
						dbCountInfo.Flow.EightHour,
						CountContent{
							Time:     seliceTime,
							Count:    dbCountInfo.Content.Count,
							ByteSize: dbCountInfo.Content.ByteSize,
						})
					dbCountInfo.Flow.EightHour = dbCountInfo.Flow.EightHour[1:]
				}
			}

			if DoDaySlice == true {
				DoDaySlice = false

				dbCountTableInfo.Day = append(
					dbCountTableInfo.Day,
					CountContent{
						Time:     seliceTime,
						Count:    dbCountTableInfo.Content.Count,
						ByteSize: dbCountTableInfo.Content.ByteSize,
					})

				//通道信息
				dbCountChannelInfo.Day = append(
					dbCountChannelInfo.Day,
					CountContent{
						Time:     seliceTime,
						Count:    dbCountChannelInfo.Content.Count,
						ByteSize: dbCountChannelInfo.Content.ByteSize,
					})

				dbCountTableInfo.Day = dbCountTableInfo.Day[1:]
				dbCountChannelInfo.Day = dbCountChannelInfo.Day[1:]

				if doDbSlice == true {

					//db总计
					dbCountInfo.Flow.Day = append(
						dbCountInfo.Flow.Day,
						CountContent{
							Time:     seliceTime,
							Count:    dbCountInfo.Content.Count,
							ByteSize: dbCountInfo.Content.ByteSize,
						})
					dbCountInfo.Flow.Day = dbCountInfo.Flow.Day[1:]
				}
			}
			dbCountTableInfo.Content.Count += data.Count
			dbCountTableInfo.Content.ByteSize += data.ByteSize

			dbCountChannelInfo.Content.Count += data.Count
			dbCountChannelInfo.Content.ByteSize += data.ByteSize

			dbCountInfo.Content.Count += data.Count
			dbCountInfo.Content.ByteSize += data.ByteSize
			dbCountInfo.Unlock()
			break
		case <-timer.C:
			timer.Reset(5 * time.Second)
			for tableId, _ := range dbCountInfo.TableMap {
				flowchan <- &FlowCount{
					//Time:"",
					Count:    0,
					TableId:  tableId,
					ByteSize: 0,
				}
			}
			break
		}
	}
}
