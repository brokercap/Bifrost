package count

import (
	"github.com/brokercap/Bifrost/config"
	"log"
	"strings"
	"sync"
	"time"
)

type FlowCount struct {
	Time     int64
	TableId  string
	Count    int64
	ByteSize int64
}

type CountContent struct {
	Time     int64
	Count    int64
	ByteSize int64
}

type CountFlow struct {
	Minute    []CountContent
	TenMinute []CountContent
	Hour      []CountContent
	EightHour []CountContent
	Day       []CountContent
	Content   *CountContent
}

type dbCountChild struct {
	sync.RWMutex
	TableMap    map[string]*CountFlow
	ChannelMap  map[string]*CountFlow
	Flow        *CountFlow
	Content     *CountContent
	doSliceTime int64
}

var l sync.RWMutex

var dbCountChanMap map[string]*dbCountChild

var dbChannelChanMap map[string]chan *FlowCount

func init() {
	dbCountChanMap = make(map[string]*dbCountChild, 0)
	dbChannelChanMap = make(map[string]chan *FlowCount)
	DoInit()
}

func DoInit() {
	go func() {
		for {
			time.Sleep(5 * time.Second)
			l.Lock()
			NowTime := time.Now().Unix()
			for _, c := range dbChannelChanMap {
				c <- &FlowCount{
					Time:  NowTime,
					Count: -3,
				}
			}
			l.Unlock()
		}
	}()
}

func SetDB(db string) {
	l.Lock()
	if _, ok := dbCountChanMap[db]; !ok {
		dbCountChanMap[db] = &dbCountChild{
			TableMap:   make(map[string]*CountFlow, 0),
			ChannelMap: make(map[string]*CountFlow, 0),
			Flow: &CountFlow{
				Minute:    flowContentInit(12),
				TenMinute: flowContentInit(120),
				Hour:      flowContentInit(120),
				EightHour: flowContentInit(96),
				Day:       flowContentInit(144),
			},
			Content: &CountContent{
				Count:    0,
				ByteSize: 0,
			},
			doSliceTime: 0,
		}
	}
	l.Unlock()
}

func DelDB(db string) {
	if _, ok := dbCountChanMap[db]; !ok {
		return
	}
	l.Lock()
	delete(dbCountChanMap, db)
	for key, c := range dbChannelChanMap {
		if strings.Index(key, db+" # ") == 0 {
			delete(dbChannelChanMap, key)
			close(c)
		}
	}
	l.Unlock()
}

func setChannelChan(key string) chan *FlowCount {
	if _, ok := dbChannelChanMap[key]; !ok {
		flowChan := make(chan *FlowCount, config.CountQueueSize)
		dbChannelChanMap[key] = flowChan
	}
	return dbChannelChanMap[key]
}

func delChannelChan(key string) {
	l.Lock()
	if dbChannelChanMap[key] != nil {
		dbChannelChanMap[key] <- &FlowCount{Count: -2}
	}
	delete(dbChannelChanMap, key)
	l.Unlock()
}

func SetChannel(db string, channelId string) chan *FlowCount {
	if _, ok := dbCountChanMap[db]; !ok {
		return nil
	}
	if _, ok := dbCountChanMap[db].ChannelMap[channelId]; !ok {
		dbCountChanMap[db].ChannelMap[channelId] = &CountFlow{
			Minute:    flowContentInit(12),
			TenMinute: flowContentInit(120),
			Hour:      flowContentInit(120),
			EightHour: flowContentInit(96),
			Day:       flowContentInit(144),
			Content: &CountContent{
				Count:    0,
				ByteSize: 0,
			},
		}
		flowChan := setChannelChan(db + " # " + channelId)
		log.Println(db, "add channelCount:", channelId)
		go channel_flowcount_sonsume(db, channelId, flowChan)
		return flowChan
	}
	return nil
}

func flowContentInit(n int) []CountContent {
	data := make([]CountContent, 0)
	for i := 0; i < n; i++ {
		data = append(data, CountContent{Time: 0, Count: 0, ByteSize: 0})
	}
	return data
}

func DelChannel(db string, channelId string) {
	if _, ok := dbCountChanMap[db]; !ok {
		return
	}
	delChannelChan(db + " # " + channelId)
	l.Lock()
	delete(dbCountChanMap[db].ChannelMap, channelId)
	l.Unlock()
	log.Println(db, "del channelCount:", channelId)
	return
}

func SetTable(db string, tableId string) {
	if _, ok := dbCountChanMap[db]; !ok {
		return
	}
	l.Lock()
	if _, ok := dbCountChanMap[db].TableMap[tableId]; !ok {
		dbCountChanMap[db].Lock()
		dbCountChanMap[db].TableMap[tableId] = &CountFlow{
			Minute:    flowContentInit(12),
			TenMinute: flowContentInit(120),
			Hour:      flowContentInit(120),
			EightHour: flowContentInit(96),
			Day:       flowContentInit(144),
			Content: &CountContent{
				Count:    0,
				ByteSize: 0,
			},
		}
		dbCountChanMap[db].Unlock()
		log.Println(db, "add table to channelCount:", tableId)
	}
	l.Unlock()
}

func DelTable(db string, tableId string) {
	if _, ok := dbCountChanMap[db]; !ok {
		return
	}
	l.Lock()
	dbCountChanMap[db].Lock()
	if _, ok := dbCountChanMap[db].TableMap[tableId]; ok {
		delete(dbCountChanMap[db].TableMap, tableId)
		log.Println(db, "del table from channelCount:", tableId)
	}
	dbCountChanMap[db].Unlock()
	l.Unlock()
}

func GetFlowByTable(db string, tableId string, flowType string) []CountContent {
	if _, ok := dbCountChanMap[db]; !ok {
		return nil
	}
	if _, ok := dbCountChanMap[db].TableMap[tableId]; !ok {
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

func GetFlowByChannel(db string, channelId string, flowType string) []CountContent {
	if _, ok := dbCountChanMap[db]; !ok {
		return nil
	}
	if _, ok := dbCountChanMap[db].ChannelMap[channelId]; !ok {
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

func GetFlowByDb(db string, flowType string) []CountContent {
	if _, ok := dbCountChanMap[db]; !ok {
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

func GetFlowAll(flowType string) []CountContent {

	var tmp []CountContent
	var result []CountContent

	switch flowType {
	case "Minute":
		result = flowContentInit(12)
		break
	case "TenMinute":
		result = flowContentInit(120)
		break
	case "Hour":
		result = flowContentInit(120)
		break
	case "EightHour":
		result = flowContentInit(96)
		break
	case "Day":
		result = flowContentInit(144)
		break
	default:
		tmp = make([]CountContent, 0)
		return tmp
	}
	for _, FlowInfo := range dbCountChanMap {
		switch flowType {
		case "Minute":
			tmp = FlowInfo.Flow.Minute[0:]
			break
		case "TenMinute":
			tmp = FlowInfo.Flow.TenMinute[0:]

			break
		case "Hour":
			tmp = FlowInfo.Flow.Hour[0:]
			break
		case "EightHour":
			tmp = FlowInfo.Flow.EightHour[0:]
			break
		case "Day":
			tmp = FlowInfo.Flow.Day[0:]
			break
		default:
			tmp = make([]CountContent, 0)
			break
		}
		for index, Flow := range tmp {
			result[index].Count += Flow.Count
			result[index].ByteSize += Flow.ByteSize
			if Flow.Time > 0 {
				result[index].Time = Flow.Time
			}
		}
	}
	return result
}
