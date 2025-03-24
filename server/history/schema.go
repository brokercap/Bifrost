package history

import (
	"database/sql/driver"
	"fmt"
	"github.com/brokercap/Bifrost/Bristol/mysql"
	"log"
	"strconv"
	"strings"
)

func DBConnect(uri string) mysql.MysqlConnection {
	db := mysql.NewConnect(uri)
	return db
}

type TableInfoStruct struct {
	TABLE_TYPE string
	ENGINE     string
	TABLE_ROWS uint64
}

type TableStruct struct {
	COLUMN_NAME       *string
	COLUMN_DEFAULT    *string
	IS_NULLABLE       *string
	COLUMN_TYPE       *string
	COLUMN_KEY        *string
	EXTRA             *string
	COLUMN_COMMENT    *string
	DATA_TYPE         *string
	NUMERIC_PRECISION *uint64
	NUMERIC_SCALE     *uint64
	Fsp               int // time,timestamp,datetime 毫秒保存的精度
}

func IsClickHouse(db mysql.MysqlConnection) (bool, error) {
	sysTableFieldList, err := GetSchemaTableFieldList(db, "system", "tables", true)
	if err != nil {
		return false, err
	}
	if len(sysTableFieldList) > 0 {
		return true, nil
	}
	return false, nil
}

func GetSchemaTableFieldList(db mysql.MysqlConnection, schema string, table string, isCK bool) ([]TableStruct, error) {
	FieldList := make([]TableStruct, 0)
	var sql string
	var valueLen int
	if isCK {
		// ck 不支持 `COLUMN_KEY`,`EXTRA`
		valueLen = 8
		sql = "SELECT `COLUMN_NAME`,`COLUMN_DEFAULT`,`IS_NULLABLE`,`COLUMN_TYPE`,`COLUMN_COMMENT`,`DATA_TYPE`,`NUMERIC_PRECISION`,`NUMERIC_SCALE` FROM `information_schema`.`columns` WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY `ORDINAL_POSITION` ASC"
	} else {
		valueLen = 10
		sql = "SELECT `COLUMN_NAME`,`COLUMN_DEFAULT`,`IS_NULLABLE`,`COLUMN_TYPE`,`COLUMN_COMMENT`,`DATA_TYPE`,`NUMERIC_PRECISION`,`NUMERIC_SCALE`,`COLUMN_KEY`,`EXTRA` FROM `information_schema`.`columns` WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY `ORDINAL_POSITION` ASC"
	}
	p := make([]driver.Value, 0)
	p = append(p, schema)
	p = append(p, table)
	rows, err := db.Query(sql, p)
	if err != nil {
		log.Printf("%v\n", err)
		return FieldList, err
	}
	defer rows.Close()

	for {
		dest := make([]driver.Value, valueLen, valueLen)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		var COLUMN_NAME string
		var COLUMN_DEFAULT *string
		var IS_NULLABLE string
		var COLUMN_TYPE string
		var COLUMN_KEY string
		var EXTRA string
		var COLUMN_COMMENT string
		var DATA_TYPE string
		var NUMERIC_PRECISION *uint64
		var NUMERIC_SCALE *uint64

		COLUMN_NAME = dest[0].(string)
		if dest[1] == nil {
			COLUMN_DEFAULT = nil
		} else {
			var t string = dest[1].(string)
			COLUMN_DEFAULT = &t
		}

		IS_NULLABLE = dest[2].(string)
		COLUMN_TYPE = dest[3].(string)
		COLUMN_COMMENT = dest[4].(string)
		DATA_TYPE = dest[5].(string)

		if dest[6] == nil {
			NUMERIC_PRECISION = nil
		} else {
			switch dest[6].(type) {
			case uint32:
				t := uint64(dest[6].(uint32))
				NUMERIC_PRECISION = &t
			case uint64:
				t := dest[6].(uint64)
				NUMERIC_PRECISION = &t
			}
		}
		if dest[7] == nil {
			NUMERIC_SCALE = nil
		} else {
			switch dest[7].(type) {
			case uint32:
				t := uint64(dest[7].(uint32))
				NUMERIC_PRECISION = &t
			case uint64:
				t := dest[7].(uint64)
				NUMERIC_PRECISION = &t
			}
		}
		if valueLen > 8 {
			COLUMN_KEY = dest[8].(string)
			EXTRA = dest[9].(string)
		}

		var fsp int
		if strings.Index(DATA_TYPE, "Nullable(") == 0 {
			DATA_TYPE = strings.TrimLeft(DATA_TYPE, "Nullable(")
			DATA_TYPE = strings.TrimRight(DATA_TYPE, ")")
		}
		switch strings.ToLower(DATA_TYPE) {
		case "timestamp", "datetime", "time", "datetime64":
			columnDataType := strings.ToLower(COLUMN_TYPE)
			i := strings.Index(columnDataType, "(")
			if i <= 0 {
				break
			}
			fsp, _ = strconv.Atoi(columnDataType[i+1 : len(columnDataType)-1])
			break
		default:
			break
		}
		if isCK {
			if IS_NULLABLE != "1" {
				IS_NULLABLE = "NO"
			} else {
				IS_NULLABLE = "YES"
			}
		}
		FieldList = append(FieldList, TableStruct{
			COLUMN_NAME:       &COLUMN_NAME,
			COLUMN_DEFAULT:    COLUMN_DEFAULT,
			IS_NULLABLE:       &IS_NULLABLE,
			COLUMN_TYPE:       &COLUMN_TYPE,
			COLUMN_KEY:        &COLUMN_KEY,
			EXTRA:             &EXTRA,
			COLUMN_COMMENT:    &COLUMN_COMMENT,
			DATA_TYPE:         &DATA_TYPE,
			NUMERIC_PRECISION: NUMERIC_PRECISION,
			NUMERIC_SCALE:     NUMERIC_SCALE,
			Fsp:               fsp,
		})
	}
	return FieldList, nil
}

func GetTablePriKeyMinAndMaxVal(db mysql.MysqlConnection, schema, table, PriKey, where string) (minId uint64, maxId uint64) {
	sql := "SELECT MIN(`" + PriKey + "`),MAX(`" + PriKey + "`) FROM `" + schema + "`.`" + table + "`"
	if where != "" {
		sql += " WHERE " + where
	}
	rows, err := db.Query(sql, []driver.Value{})
	if err != nil {
		log.Printf("%v\n", err)
	}
	defer rows.Close()
	for {
		var err error
		dest := make([]driver.Value, 2, 2)
		err = rows.Next(dest)
		if err != nil {
			break
		}
		minId, err = strconv.ParseUint(fmt.Sprint(dest[0]), 10, 64)
		maxId, err = strconv.ParseUint(fmt.Sprint(dest[1]), 10, 64)
		break
	}
	return
}

func GetSchemaTableInfo(db mysql.MysqlConnection, schema string, table string) (tableInfo TableInfoStruct) {
	sql := "SELECT `TABLE_TYPE`,`ENGINE`,`TABLE_ROWS` FROM information_schema.tables WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
	p := make([]driver.Value, 0)
	p = append(p, schema)
	p = append(p, table)
	rows, err := db.Query(sql, p)
	if err != nil {
		return
	}
	defer rows.Close()
	if err != nil {
		log.Printf("%v\n", err)
		return tableInfo
	}

	for {
		dest := make([]driver.Value, 3, 3)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		var TABLE_TYPE string
		var ENGINE string
		var TABLE_ROWS uint64

		if dest[0] == nil {
			TABLE_TYPE = ""
		} else {
			TABLE_TYPE = dest[0].(string)
		}
		if dest[1] == nil {
			ENGINE = ""
		} else {
			ENGINE = dest[1].(string)
		}

		if dest[2] == nil {
			TABLE_ROWS = 0
		} else {
			switch dest[2].(type) {
			// starrocks或者其他一些数据库等返回的是int64
			case int64:
				TABLE_ROWS = uint64(dest[2].(int64))
			case uint64:
				TABLE_ROWS = dest[2].(uint64)
			default:
				TABLE_ROWS = 1
			}
		}
		tableInfo = TableInfoStruct{
			TABLE_TYPE: TABLE_TYPE,
			ENGINE:     ENGINE,
			TABLE_ROWS: TABLE_ROWS,
		}
		break
	}
	return
}
