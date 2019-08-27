package mysql

import (
	"strconv"
	"database/sql/driver"
	"fmt"
)

func CheckBinlogIsRight(dbUri string,filename string, position uint32) error{
	db := NewConnect(dbUri)
	sql := "show binlog events IN '"+filename+"' FROM "+ strconv.FormatInt(int64(position),10) +" LIMIT 1"
	stmt,err := db.Prepare(sql)
	if err !=nil{
		return err
	}
	defer stmt.Close()
	rows, err := stmt.Query([]driver.Value{})
	defer rows.Close()
	if err != nil {
		return err
	}
	var returnErr error
	for {
		dest := make([]driver.Value, 6, 6)
		err := rows.Next(dest)
		if err != nil {
			break
		}

		Event_type := dest[3].(string)
		switch Event_type {
		case "row_event","insert_event","update_event","delete_event":
			returnErr = fmt.Errorf("binlog position cant'be "+Event_type)
			break
		default:
			break
		}
		break

	}
	return returnErr
}

func GetNearestRightBinlog(dbUri string,filename string, position uint32,serverId uint32,ReplicateDoDb map[string]uint8) (uint32){
	binlogDump := &BinlogDump{
		DataSource:    	dbUri,
		ReplicateDoDb: 	ReplicateDoDb,
		OnlyEvent:     	[]EventType{
			QUERY_EVENT,TABLE_MAP_EVENT,
		},
	}

	var nearestPosition uint32

	var Callback = func (data *EventReslut) {
		nearestPosition = data.Header.LogPos
	}
	reslut := make(chan error, 1)
	binlogDump.CallbackFun = Callback
	go binlogDump.StartDumpBinlog(filename, 4, serverId, reslut, filename, position)
	for{
		r := <- reslut
		if r.Error() != "running" && r.Error() != "starting" {
			break
		}
	}
	return nearestPosition
}