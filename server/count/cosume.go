package count

import (
	"time"
	"log"
	"runtime"
)

func channel_flowcount_sonsume(db string,channelId string,flowchan chan *FlowCount){
	/*
	defer func() {
		if err:=recover();err!=nil{
			delChannelChan(db+"-"+channelId)
			log.Println(db,channelId," channel_flowcount_sonsume recover: ",err)
		}
	}()
	*/
	//var Minute,TenMinute,Hour,EightHour,Day uint = 0,0,0,0,0
	log.Println(db,channelId,"channel count start")
	var DoMinuteSlice bool = false
	var DoTenMinuteSlice bool = false
	var DoHourSlice bool = false
	var DoEightHourSlice bool = false
	var DoDaySlice bool = false

	var fori uint = 0
	var seliceTime *string
	nowTime := time.Now().Format("2006-01-02 15:04:05")
	seliceTime = &nowTime
	var doDbSlice bool = true
	for {
		data := <- flowchan
		if data == nil{
			runtime.Goexit()
			break
		}
		if data.Count < 0 {
			if data.Count == -2 {
				log.Println(db, channelId, "channel close")
				runtime.Goexit()
				break
			}
			if data.Count == -3 {
				//count DoInit 里的协程定时，每5秒往这个chan里发送一条信息
				dbCountChanMap[db].Lock()
				if dbCountChanMap[db].doSliceTime == data.Time {
					doDbSlice = false
				} else {
					dbCountChanMap[db].doSliceTime = data.Time
					doDbSlice = true
				}
				dbCountChanMap[db].Unlock()
				seliceTime = &data.Time
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
		dbCountChanMap[db].Lock()
		if _,ok:=dbCountChanMap[db].TableMap[*data.TableId];!ok{
			continue
		}
		if DoMinuteSlice == true{
			DoMinuteSlice = false
			dbCountChanMap[db].TableMap[*data.TableId].Minute = dbCountChanMap[db].TableMap[*data.TableId].Minute[1:]
			dbCountChanMap[db].ChannelMap[channelId].Minute = dbCountChanMap[db].ChannelMap[channelId].Minute[1:]

			if doDbSlice == true{
				dbCountChanMap[db].Flow.Minute = dbCountChanMap[db].Flow.Minute[1:]
				//db总计
				dbCountChanMap[db].Flow.Minute = append(
					dbCountChanMap[db].Flow.Minute,
					CountContent{
						Time:     *seliceTime,
						Count:    dbCountChanMap[db].Content.Count,
						ByteSize: dbCountChanMap[db].Content.ByteSize,
					})
			}

			//表的统计信息
			dbCountChanMap[db].TableMap[*data.TableId].Minute = append(
				dbCountChanMap[db].TableMap[*data.TableId].Minute,
				CountContent{
					Time:     *seliceTime,
					Count:    dbCountChanMap[db].TableMap[*data.TableId].Content.Count,
					ByteSize: dbCountChanMap[db].TableMap[*data.TableId].Content.ByteSize,
				})

			//通道信息
			dbCountChanMap[db].ChannelMap[channelId].Minute = append(
				dbCountChanMap[db].ChannelMap[channelId].Minute,
				CountContent{
					Time:     *seliceTime,
					Count:    dbCountChanMap[db].ChannelMap[channelId].Content.Count,
					ByteSize: dbCountChanMap[db].ChannelMap[channelId].Content.ByteSize,
				})



		}


		if DoTenMinuteSlice == true {
			DoTenMinuteSlice = false
			dbCountChanMap[db].TableMap[*data.TableId].TenMinute = dbCountChanMap[db].TableMap[*data.TableId].TenMinute[1:]
			dbCountChanMap[db].ChannelMap[channelId].TenMinute = dbCountChanMap[db].ChannelMap[channelId].TenMinute[1:]


			dbCountChanMap[db].TableMap[*data.TableId].TenMinute = append(
				dbCountChanMap[db].TableMap[*data.TableId].TenMinute,
				CountContent{
					Time:     *seliceTime,
					Count:    dbCountChanMap[db].TableMap[*data.TableId].Content.Count,
					ByteSize: dbCountChanMap[db].TableMap[*data.TableId].Content.ByteSize,
				})

			//通道信息
			dbCountChanMap[db].ChannelMap[channelId].TenMinute = append(
				dbCountChanMap[db].ChannelMap[channelId].TenMinute,
				CountContent{
					Time:     *seliceTime,
					Count:    dbCountChanMap[db].ChannelMap[channelId].Content.Count,
					ByteSize: dbCountChanMap[db].ChannelMap[channelId].Content.ByteSize,
				})

			if doDbSlice == true {
				dbCountChanMap[db].Flow.TenMinute = dbCountChanMap[db].Flow.TenMinute[1:]
				//db总计
				dbCountChanMap[db].Flow.TenMinute = append(
					dbCountChanMap[db].Flow.TenMinute,
					CountContent{
						Time:     *seliceTime,
						Count:    dbCountChanMap[db].Content.Count,
						ByteSize: dbCountChanMap[db].Content.ByteSize,
					})
			}

		}

			//每30秒一条数据
		if DoHourSlice == true{
			DoHourSlice = false
			dbCountChanMap[db].TableMap[*data.TableId].Hour = dbCountChanMap[db].TableMap[*data.TableId].Hour[1:]
			dbCountChanMap[db].ChannelMap[channelId].Hour = dbCountChanMap[db].ChannelMap[channelId].Hour[1:]


			dbCountChanMap[db].TableMap[*data.TableId].Hour = append(
				dbCountChanMap[db].TableMap[*data.TableId].Hour,
				CountContent{
					Time:*seliceTime,
					Count:dbCountChanMap[db].TableMap[*data.TableId].Content.Count,
					ByteSize:dbCountChanMap[db].TableMap[*data.TableId].Content.ByteSize,
				})

			//通道信息
			dbCountChanMap[db].ChannelMap[channelId].Hour = append(
				dbCountChanMap[db].ChannelMap[channelId].Hour,
				CountContent{
					Time:*seliceTime,
					Count:dbCountChanMap[db].ChannelMap[channelId].Content.Count,
					ByteSize:dbCountChanMap[db].ChannelMap[channelId].Content.ByteSize,
				})

			if doDbSlice == true {
				dbCountChanMap[db].Flow.Hour = dbCountChanMap[db].Flow.Hour[1:]

				//db总计
				dbCountChanMap[db].Flow.Hour = append(
					dbCountChanMap[db].Flow.Hour,
					CountContent{
						Time:     *seliceTime,
						Count:    dbCountChanMap[db].Content.Count,
						ByteSize: dbCountChanMap[db].Content.ByteSize,
					})
			}
		}
		if DoEightHourSlice == true {
			DoEightHourSlice = false
			//每5分钟一条数据

			dbCountChanMap[db].TableMap[*data.TableId].EightHour = dbCountChanMap[db].TableMap[*data.TableId].EightHour[1:]
			dbCountChanMap[db].ChannelMap[channelId].EightHour = dbCountChanMap[db].ChannelMap[channelId].EightHour[1:]


			dbCountChanMap[db].TableMap[*data.TableId].EightHour = append(
				dbCountChanMap[db].TableMap[*data.TableId].EightHour,
				CountContent{
					Time:*seliceTime,
					Count:dbCountChanMap[db].TableMap[*data.TableId].Content.Count,
					ByteSize:dbCountChanMap[db].TableMap[*data.TableId].Content.ByteSize,
				})


			//通道信息
			dbCountChanMap[db].ChannelMap[channelId].EightHour = append(
				dbCountChanMap[db].ChannelMap[channelId].EightHour,
				CountContent{
					Time:*seliceTime,
					Count:dbCountChanMap[db].ChannelMap[channelId].Content.Count,
					ByteSize:dbCountChanMap[db].ChannelMap[channelId].Content.ByteSize,
				})


			if doDbSlice == true {
				dbCountChanMap[db].Flow.EightHour = dbCountChanMap[db].Flow.EightHour[1:]
				//db总计
				dbCountChanMap[db].Flow.Hour = append(
					dbCountChanMap[db].Flow.Hour,
					CountContent{
						Time:     *seliceTime,
						Count:    dbCountChanMap[db].Content.Count,
						ByteSize: dbCountChanMap[db].Content.ByteSize,
					})
			}
		}

		if DoDaySlice == true {
			DoDaySlice = false

			dbCountChanMap[db].TableMap[*data.TableId].Day = dbCountChanMap[db].TableMap[*data.TableId].Day[1:]
			dbCountChanMap[db].ChannelMap[channelId].Day = dbCountChanMap[db].ChannelMap[channelId].Day[1:]


			dbCountChanMap[db].TableMap[*data.TableId].Day = append(
				dbCountChanMap[db].TableMap[*data.TableId].Day,
				CountContent{
					Time:*seliceTime,
					Count:dbCountChanMap[db].TableMap[*data.TableId].Content.Count,
					ByteSize:dbCountChanMap[db].TableMap[*data.TableId].Content.ByteSize,
				})

			//通道信息
			dbCountChanMap[db].ChannelMap[channelId].Day = append(
				dbCountChanMap[db].ChannelMap[channelId].Day,
				CountContent{
					Time:*seliceTime,
					Count:dbCountChanMap[db].ChannelMap[channelId].Content.Count,
					ByteSize:dbCountChanMap[db].ChannelMap[channelId].Content.ByteSize,
				})

			if doDbSlice == true {
				dbCountChanMap[db].Flow.Day = dbCountChanMap[db].Flow.Day[1:]
				//db总计
				dbCountChanMap[db].Flow.Day = append(
					dbCountChanMap[db].Flow.Day,
					CountContent{
						Time:     *seliceTime,
						Count:    dbCountChanMap[db].Content.Count,
						ByteSize: dbCountChanMap[db].Content.ByteSize,
					})
			}
		}
		dbCountChanMap[db].TableMap[*data.TableId].Content.Count += data.Count
		dbCountChanMap[db].TableMap[*data.TableId].Content.ByteSize += data.ByteSize

		dbCountChanMap[db].ChannelMap[channelId].Content.Count += data.Count
		dbCountChanMap[db].ChannelMap[channelId].Content.ByteSize += data.ByteSize

		dbCountChanMap[db].Content.Count += data.Count
		dbCountChanMap[db].Content.ByteSize += data.ByteSize
		dbCountChanMap[db].Unlock()
	}
}
