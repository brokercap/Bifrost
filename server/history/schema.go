package history


import (
	"github.com/brokercap/Bifrost/Bristol/mysql"
	"database/sql/driver"
	"log"
	"strconv"
	"fmt"
)

func DBConnect(uri string) mysql.MysqlConnection{
	db := mysql.NewConnect(uri)
	return db
}

type TableInfoStruct struct {
	TABLE_TYPE 		string
	ENGINE 			string
	TABLE_ROWS 		uint64
}

type TableStruct struct {
	COLUMN_NAME 		*string
	COLUMN_DEFAULT 		*string
	IS_NULLABLE 		*string
	COLUMN_TYPE			*string
	COLUMN_KEY 			*string
	EXTRA 				*string
	COLUMN_COMMENT 		*string
	DATA_TYPE			*string
	NUMERIC_PRECISION	*uint64
	NUMERIC_SCALE		*uint64
}

func GetSchemaTableFieldList(db mysql.MysqlConnection,schema string,table string) []TableStruct{

	FieldList := make([]TableStruct,0)
	sql := "SELECT `COLUMN_NAME`,`COLUMN_DEFAULT`,`IS_NULLABLE`,`COLUMN_TYPE`,`COLUMN_KEY`,`EXTRA`,`COLUMN_COMMENT`,`DATA_TYPE`,`NUMERIC_PRECISION`,`NUMERIC_SCALE` FROM `information_schema`.`columns` WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY `ORDINAL_POSITION` ASC"
	stmt,err := db.Prepare(sql)
	if err !=nil{
		log.Println(err)
		return FieldList
	}
	defer stmt.Close()
	p := make([]driver.Value, 0)
	p = append(p,schema)
	p = append(p,table)
	rows, err := stmt.Query(p)
	defer rows.Close()
	if err != nil {
		log.Printf("%v\n", err)
		return FieldList
	}

	for {
		dest := make([]driver.Value, 10, 10)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		var COLUMN_NAME 		string
		var COLUMN_DEFAULT 		*string
		var IS_NULLABLE 		string
		var COLUMN_TYPE 		string
		var COLUMN_KEY 			string
		var EXTRA 				string
		var COLUMN_COMMENT 		string
		var DATA_TYPE 			string
		var NUMERIC_PRECISION 	*uint64
		var NUMERIC_SCALE 		*uint64

		COLUMN_NAME 		= dest[0].(string)
		if dest[1] == nil{
			COLUMN_DEFAULT 	= nil
		}else{
			var t string =  dest[1].(string)
			COLUMN_DEFAULT = &t
		}

		IS_NULLABLE 		= dest[2].(string)
		COLUMN_TYPE 		= dest[3].(string)
		COLUMN_KEY 			= dest[4].(string)
		EXTRA 				= dest[5].(string)
		COLUMN_COMMENT 		= dest[6].(string)
		DATA_TYPE 			= dest[7].(string)

		if dest[8] == nil{
			NUMERIC_PRECISION 	= nil
		}else{
			switch dest[8].(type) {
			case uint32:
				t := uint64(dest[8].(uint32))
				NUMERIC_PRECISION = &t
			case uint64:
				t := dest[8].(uint64)
				NUMERIC_PRECISION 	= &t
			}
		}
		if dest[9] == nil{
			NUMERIC_SCALE 	= nil
		}else{
			switch dest[9].(type) {
			case uint32:
				t := uint64(dest[9].(uint32))
				NUMERIC_PRECISION = &t
			case uint64:
				t := dest[9].(uint64)
				NUMERIC_PRECISION 	= &t
			}
		}

		FieldList = append(FieldList,TableStruct{
			COLUMN_NAME:	&COLUMN_NAME,
			COLUMN_DEFAULT:	COLUMN_DEFAULT,
			IS_NULLABLE:	&IS_NULLABLE,
			COLUMN_TYPE:	&COLUMN_TYPE,
			COLUMN_KEY:		&COLUMN_KEY,
			EXTRA:			&EXTRA,
			COLUMN_COMMENT:	&COLUMN_COMMENT,
			DATA_TYPE:		&DATA_TYPE,
			NUMERIC_PRECISION:NUMERIC_PRECISION,
			NUMERIC_SCALE:	NUMERIC_SCALE,
		})
	}
	return FieldList
}

func GetTablePriKeyMinAndMaxVal(db mysql.MysqlConnection,schema,table,PriKey,where string) (minId uint64,maxId uint64){
	sql := "SELECT MIN(`"+PriKey+"`),MAX(`"+PriKey+"`) FROM `"+schema+"`.`"+table+"`"
	if where != ""{
		sql += " WHERE "+where
	}
	stmt,err := db.Prepare(sql)
	if err !=nil{
		log.Println(err," sql:",sql)
		return
	}
	defer stmt.Close()
	rows, err := stmt.Query([]driver.Value{})
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


func GetSchemaTableInfo(db mysql.MysqlConnection,schema string,table string) (tableInfo TableInfoStruct){
	sql := "SELECT `TABLE_TYPE`,`ENGINE`,`TABLE_ROWS` FROM information_schema.tables WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
	stmt,err := db.Prepare(sql)
	if err !=nil{
		log.Println(err)
		return tableInfo
	}
	defer stmt.Close()
	p := make([]driver.Value, 0)
	p = append(p,schema)
	p = append(p,table)
	rows, err := stmt.Query(p)
	if err != nil{
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
		var TABLE_TYPE 		string
		var ENGINE 			string
		var TABLE_ROWS 		uint64

		if dest[0] == nil {
			TABLE_TYPE = ""
		}else{
			TABLE_TYPE 		= dest[0].(string)
		}
		if dest[1] == nil {
			ENGINE = ""
		}else {
			ENGINE 			= dest[1].(string)
		}

		if dest[2] == nil{
			TABLE_ROWS 		= 0
		}else{
			TABLE_ROWS 		= dest[2].(uint64)
		}
		tableInfo = TableInfoStruct{
			TABLE_TYPE:TABLE_TYPE,
			ENGINE:ENGINE,
			TABLE_ROWS:TABLE_ROWS,
		}
		break
	}
	return
}