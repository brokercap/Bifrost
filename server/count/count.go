package count

import (
	"time"
	"sync"
)

type FlowCount struct {
	Time string
	TableId *string
	Count int64
	ByteSize int64
}

type CountContent struct {
	Time string
	Count int64
	ByteSize int64
}

type CountFlow struct {
	Minute []CountContent
	TenMinute []CountContent
	Hour []CountContent
	EightHour []CountContent
	Day []CountContent
	Content *CountContent
}

type dbCountChild struct {
    sync.RWMutex
	TableMap map[string]*CountFlow
	ChannelMap map[string]*CountFlow
	Flow *CountFlow
	Content *CountContent
}

var l sync.RWMutex

var dbCountChanMap map[string]*dbCountChild

var dbChannelChanMap map[string]chan *FlowCount

func init(){
	dbCountChanMap = make(map[string]*dbCountChild,0)
	dbChannelChanMap = make(map[string]chan *FlowCount)
	DoInit()
}

func DoInit(){
	go func() {
		for{
			time.Sleep(5 * time.Second)
			for _,c := range dbChannelChanMap{
				c <- &FlowCount{
					Time:time.Now().Format("2006-01-02 15:04:05"),
					Count:-1,
				}
			}
		}
	}()
}

func SetDB(db string){
	l.Lock()
	if _,ok := dbCountChanMap[db];!ok{
		dbCountChanMap[db] = &dbCountChild{
			TableMap:make(map[string]*CountFlow,0),
			ChannelMap:make(map[string]*CountFlow,0),
			Flow:&CountFlow{
				Minute:flowContentInit(12),
				TenMinute:flowContentInit(120),
				Hour:flowContentInit(120),
				EightHour:flowContentInit(96),
				Day:flowContentInit(144),
			},
			Content:&CountContent{
				Count:0,
				ByteSize:0,
			},
		}
	}
	l.Unlock()
}

func DelDB(db string){
	if _,ok := dbCountChanMap[db];!ok{
		return
	}
	l.Lock()
	delete(dbCountChanMap,db)
	l.Unlock()
}

func setChannelChan(key string) chan *FlowCount{
	if _,ok := dbChannelChanMap[key];!ok{
		flowChan := make(chan *FlowCount,1000)
		dbChannelChanMap[key] = flowChan
	}
	return dbChannelChanMap[key]
}

func delChannelChan(key string){
	l.Lock()
	dbChannelChanMap[key] <- &FlowCount{Count:-2}
	delete(dbChannelChanMap,key)
	l.Unlock()
}

func SetChannel(db string,channelId string) chan *FlowCount{
	if _,ok := dbCountChanMap[db];!ok{
		return nil
	}
	if _,ok := dbCountChanMap[db].ChannelMap[channelId];!ok{
		dbCountChanMap[db].ChannelMap[channelId] = &CountFlow{
			Minute:flowContentInit(12),
			TenMinute:flowContentInit(120),
			Hour:flowContentInit(120),
			EightHour:flowContentInit(96),
			Day:flowContentInit(144),
			Content:&CountContent{
				Count:0,
				ByteSize:0,
			},
		}
		flowChan := setChannelChan(db+"-"+channelId)
		go channel_flowcount_sonsume(db,channelId,flowChan)
		return flowChan
	}
	return nil
}

func flowContentInit(n int) []CountContent{
	data := make([]CountContent,0)
	for i:=0;i<n;i++ {
		data = append(data,CountContent{Time:"", Count:0, ByteSize:0})
	}
	return data
}

func DelChannel(db string,channelId string){
	if _,ok := dbCountChanMap[db];!ok{
		return
	}
	l.Lock()
	delete(dbCountChanMap[db].ChannelMap,channelId)
	l.Unlock()
	delChannelChan(db+"-"+channelId)
	return
}

func SetTable(db string,tableId string){
	if _,ok := dbCountChanMap[db];!ok{
		return
	}
	l.Lock()
	if _,ok := dbCountChanMap[db].TableMap[tableId];!ok{
		dbCountChanMap[db].TableMap[tableId] = &CountFlow{
			Minute:flowContentInit(12),
			TenMinute:flowContentInit(120),
			Hour:flowContentInit(120),
			EightHour:flowContentInit(96),
			Day:flowContentInit(144),
			Content:&CountContent{
				Count:0,
				ByteSize:0,
			},
		}
	}
	l.Unlock()
}

func GetFlowByTable(db string,tableId string,flowType string) []CountContent{
	if _,ok := dbCountChanMap[db];!ok{
		return nil
	}
	if _,ok := dbCountChanMap[db].TableMap[tableId];!ok{
		return nil
	}
	switch flowType {
	case "Minute":
		return dbCountChanMap[db].TableMap[tableId].Minute
		break
	case "TenMinute":
		return dbCountChanMap[db].TableMap[tableId].TenMinute
		break
	case "Hour":
		return dbCountChanMap[db].TableMap[tableId].Hour
		break
	case "EightHour":
		return dbCountChanMap[db].TableMap[tableId].EightHour
		break
	case "Day":
		return dbCountChanMap[db].TableMap[tableId].Day
		break
	default:
		break
	}
	return nil
}

func GetFlowByChannel(db string,channelId string,flowType string) []CountContent{
	if _,ok := dbCountChanMap[db];!ok{
		return nil
	}
	if _,ok := dbCountChanMap[db].ChannelMap[channelId];!ok{
		return nil
	}
	switch flowType {
	case "Minute":
		return dbCountChanMap[db].ChannelMap[channelId].Minute
		break
	case "TenMinute":
		return dbCountChanMap[db].ChannelMap[channelId].TenMinute
		break
	case "Hour":
		return dbCountChanMap[db].ChannelMap[channelId].Hour
		break
	case "EightHour":
		return dbCountChanMap[db].ChannelMap[channelId].EightHour
		break
	case "Day":
		return dbCountChanMap[db].ChannelMap[channelId].Day
		break
	default:
		break
	}
	return nil
}

func GetFlowByDb(db string,flowType string) []CountContent{
	if _,ok := dbCountChanMap[db];!ok{
		return nil
	}
	switch flowType {
	case "Minute":
		return dbCountChanMap[db].Flow.Minute[0:]
		break
	case "TenMinute":
		return dbCountChanMap[db].Flow.TenMinute[0:]
		break
	case "Hour":
		return dbCountChanMap[db].Flow.Hour[0:]
		break
	case "EightHour":
		return dbCountChanMap[db].Flow.EightHour[0:]
		break
	case "Day":
		return dbCountChanMap[db].Flow.Day[0:]
		break
	default:
		break
	}
	return nil
}