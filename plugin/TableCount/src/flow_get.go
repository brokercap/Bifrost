package src

import (
	"fmt"
	"time"
)

// 过滤时间不对的数据
func fitlerFlow(data []CountContent, timeDiff int64, listCount int) []CountContent {
	NowTime := time.Now().Unix()
	NowTime = NowTime - NowTime%5
	var i = 0
	for k, Count := range data {
		if NowTime-Count.Time <= timeDiff {
			i = k
			break
		}
	}
	if i > 0 {
		data = data[i:]
	}
	n := len(data)
	n0 := listCount - n
	if n0 > 0 {
		var lastTime int64
		if n > 0 {
			lastTime = data[n-1].Time
		} else {
			lastTime = NowTime
		}
		everBlankTime := timeDiff / int64(listCount)

		n2 := int((NowTime - lastTime) / everBlankTime)
		n1 := listCount - n - n2
		if n1 < 0 {
			return data
		}
		tmp := make([]CountContent, n1)
		for i := 0; i < n1; i++ {
			tmp[i].Time = lastTime - everBlankTime*int64(n1-i)
		}

		tmp2 := make([]CountContent, n2)
		for i := 0; i < n2; i++ {
			tmp2[i].Time = lastTime + everBlankTime*int64(i+1)
		}
		data = append(tmp, data...)
		data = append(data, tmp2...)
	}
	return data
}

// 将多个表的流量合并起来
func sumFlow(dest []CountContent, src []CountContent) []CountContent {
	if len(dest) == 0 {
		dest = src
		return dest
	}
	for i, Count := range src {
		if dest[i].Time == 0 {
			dest[i].Time = Count.Time
		}
		dest[i].InsertCount += Count.InsertCount
		dest[i].UpdateCount += Count.UpdateCount
		dest[i].DeleteCount += Count.DeleteCount
		dest[i].InsertRows += Count.InsertRows
		dest[i].UpdateRows += Count.UpdateRows
		dest[i].DeleteRows += Count.DeleteRows
		dest[i].DDLCount += Count.DDLCount
	}
	return dest
}

func GetFlow(flowType string, dbname, schameName, tableName string) (data []CountContent, err error) {
	i := getDBArrI(dbname)
	t := dbArr[i]
	t.RLock()
	defer t.RUnlock()
	if _, ok := t.dbMap[dbname]; !ok {
		err = fmt.Errorf("dbname:" + dbname + " not exsit")
		return
	}
	db := t.dbMap[dbname]
	if _, ok := db.schemaMap[schameName]; !ok {
		err = fmt.Errorf("schameName:" + schameName + " not exsit")
		return
	}

	if _, ok := db.schemaMap[schameName][tableName]; !ok {
		err = fmt.Errorf("tableName:" + tableName + " not exsit")
		return
	}

	switch flowType {
	case "TenMinute":
		return fitlerFlow(db.schemaMap[schameName][tableName].TenMinute.Data, 600, 120), nil
		break
	case "Hour":
		return fitlerFlow(db.schemaMap[schameName][tableName].Hour.Data, 3600, 120), nil
		break
	case "EightHour":
		return fitlerFlow(db.schemaMap[schameName][tableName].EightHour.Data, 28800, 96), nil
		break
	case "Day":
		return fitlerFlow(db.schemaMap[schameName][tableName].Day.Data, 86400, 144), nil
		break
	default:
		err = fmt.Errorf("flowType not esxit")
		break
	}
	return
}

func GetFlowBySchema(flowType string, dbname, schameName string) (data []CountContent, err error) {
	i := getDBArrI(dbname)
	t := dbArr[i]
	t.RLock()
	defer t.RUnlock()
	if _, ok := t.dbMap[dbname]; !ok {
		err = fmt.Errorf("dbname:" + dbname + " not exsit")
		return
	}
	db := t.dbMap[dbname]
	if _, ok := db.schemaMap[schameName]; !ok {
		err = fmt.Errorf("schameName:" + schameName + " not exsit")
		return
	}

	switch flowType {
	case "TenMinute":
		for _, tableFlowInfo := range db.schemaMap[schameName] {
			d := fitlerFlow(tableFlowInfo.TenMinute.Data, 600, 120)
			data = sumFlow(data, d)
		}
		break
	case "Hour":
		for _, tableFlowInfo := range db.schemaMap[schameName] {
			d := fitlerFlow(tableFlowInfo.Hour.Data, 3600, 120)
			data = sumFlow(data, d)
		}
		break
	case "EightHour":
		for _, tableFlowInfo := range db.schemaMap[schameName] {
			d := fitlerFlow(tableFlowInfo.EightHour.Data, 28800, 96)
			data = sumFlow(data, d)
		}
		break
	case "Day":
		for _, tableFlowInfo := range db.schemaMap[schameName] {
			d := fitlerFlow(tableFlowInfo.Day.Data, 86400, 144)
			data = sumFlow(data, d)
		}
		break
	default:
		err = fmt.Errorf("flowType not esxit")
		break
	}
	return
}

func GetFlowByDbName(flowType string, dbname string) (data []CountContent, err error) {
	i := getDBArrI(dbname)
	t := dbArr[i]
	t.RLock()
	defer t.RUnlock()
	if _, ok := t.dbMap[dbname]; !ok {
		err = fmt.Errorf("dbname:" + dbname + " not exsit")
		return
	}
	db := t.dbMap[dbname]

	switch flowType {
	case "TenMinute":
		for _, schemaList := range db.schemaMap {
			for _, tableFlowInfo := range schemaList {
				d := fitlerFlow(tableFlowInfo.TenMinute.Data, 600, 120)
				data = sumFlow(data, d)
			}
		}
		break
	case "Hour":
		for _, schemaList := range db.schemaMap {
			for _, tableFlowInfo := range schemaList {
				d := fitlerFlow(tableFlowInfo.Hour.Data, 3600, 120)
				data = sumFlow(data, d)
			}
		}
		break
	case "EightHour":
		for _, schemaList := range db.schemaMap {
			for _, tableFlowInfo := range schemaList {
				d := fitlerFlow(tableFlowInfo.EightHour.Data, 28800, 96)
				data = sumFlow(data, d)
			}
		}
		break
	case "Day":
		for _, schemaList := range db.schemaMap {
			for _, tableFlowInfo := range schemaList {
				d := fitlerFlow(tableFlowInfo.Day.Data, 86400, 144)
				data = sumFlow(data, d)
			}
		}
		break
	default:
		err = fmt.Errorf("flowType not esxit")
		break
	}
	return
}

func GetDbList() (data []string) {
	for _, db := range dbArr {
		db.RLock()
		for _, dbInfo := range db.dbMap {
			data = append(data, dbInfo.Name)
		}
		db.RUnlock()
	}
	return
}

func GetSchameList(dbname string) (data []string) {
	i := getDBArrI(dbname)
	t := dbArr[i]
	t.RLock()
	defer t.RUnlock()
	if _, ok := t.dbMap[dbname]; !ok {
		return
	}
	for key, _ := range t.dbMap[dbname].schemaMap {
		data = append(data, key)
	}
	return
}

func GetSchameTableList(dbname string, schema string) (data []string) {
	i := getDBArrI(dbname)
	t := dbArr[i]
	t.RLock()
	defer t.RUnlock()
	if _, ok := t.dbMap[dbname]; !ok {
		return
	}
	if _, ok := t.dbMap[dbname].schemaMap[schema]; !ok {
		return
	}
	for key, _ := range t.dbMap[dbname].schemaMap[schema] {
		data = append(data, key)
	}
	return
}
