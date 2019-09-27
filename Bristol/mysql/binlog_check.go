package mysql

import (
	"strconv"
	"database/sql/driver"
	"fmt"
)

func CheckBinlogIsRight(dbUri string,filename string, position uint32) error{
	db := NewConnect(dbUri)
	defer db.Close()
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

		Event_type :=string(dest[2].([]byte))

		switch Event_type {
		case "Update_rows","Delete_rows","Insert_rows","Update_rows_v1","Delete_rows_v1","Insert_rows_v1","Update_rows_v0","Delete_rows_v0","Insert_rows_v0","Update_rows_v2","Delete_rows_v2","Insert_rows_v2":
			returnErr = fmt.Errorf("binlog position cant'be "+Event_type)
			break
		default:
			break
		}
		break

	}
	return returnErr
}

func GetNearestRightBinlog(dbUri string,filename string, position uint32,serverId uint32,ReplicateDoDb map[string]map[string]uint8,ReplicateIgnoreDb map[string]map[string]uint8) (uint32){
	var nearestPosition uint32 = 4
	var Callback = func (data *EventReslut) {
		nearestPosition = data.Header.LogPos
	}
	binlogDump := NewBinlogDump(
		dbUri,
		Callback,
		[]EventType{
			QUERY_EVENT,TABLE_MAP_EVENT,
		},
		ReplicateDoDb,
		ReplicateIgnoreDb)

	reslut := make(chan error, 1)
	//binlogDump.CallbackFun = Callback
	go binlogDump.StartDumpBinlog(filename, 4, serverId, reslut, filename, position)
	for{
		r := <- reslut
		if r.Error() != "running" && r.Error() != "starting" {
			break
		}
	}
	return nearestPosition
}