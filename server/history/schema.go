package history


import (
	"github.com/brokercap/Bifrost/Bristol/mysql"
	"database/sql/driver"
	"log"
	"github.com/brokercap/Bifrost/util/dataType"
	"strings"
)

func DBConnect(uri string) mysql.MysqlConnection{
	db := mysql.NewConnect(uri)
	return db
}

type TableStruct struct {
	COLUMN_NAME 		string
	COLUMN_DEFAULT 		string
	IS_NULLABLE 		string
	COLUMN_TYPE			string
	COLUMN_KEY 			string
	EXTRA 				string
	COLUMN_COMMENT 		string
	DATA_TYPE			string
	NUMERIC_PRECISION	string
	NUMERIC_SCALE		string
	ToDataType			dataType.Type
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
		var COLUMN_NAME string
		var COLUMN_DEFAULT string
		var IS_NULLABLE string
		var COLUMN_TYPE string
		var COLUMN_KEY string
		var EXTRA string
		var COLUMN_COMMENT string
		var DATA_TYPE string
		var NUMERIC_PRECISION string
		var NUMERIC_SCALE string

		COLUMN_NAME 		= string(dest[0].([]byte))
		if dest[1] == nil{
			COLUMN_DEFAULT 	= "NULL"
		}else{
			COLUMN_DEFAULT 	= string(dest[1].([]byte))
		}

		IS_NULLABLE 		= string(dest[2].([]byte))
		COLUMN_TYPE 		= string(dest[3].([]byte))
		COLUMN_KEY 			= string(dest[4].([]byte))
		EXTRA 				= string(dest[5].([]byte))
		COLUMN_COMMENT 		= string(dest[6].([]byte))
		DATA_TYPE 			= string(dest[7].([]byte))

		if dest[8] == nil{
			NUMERIC_PRECISION 	= "NULL"
		}else{
			NUMERIC_PRECISION 	= string(dest[8].([]byte))
		}
		if dest[9] == nil{
			NUMERIC_SCALE 	= "NULL"
		}else{
			NUMERIC_SCALE 	= string(dest[9].([]byte))
		}

		var ToDataType dataType.Type
		switch DATA_TYPE {
		case "char","varchar","set","enum","text","blob","mediumblob","longblob","tinyblob","mediumtext","longtext","tinytext","time","date","datetime","timestamp":
			ToDataType = dataType.STRING_TYPE
			break
		case "tinyint":
			if COLUMN_TYPE == "tinyint(1)"{
				ToDataType = dataType.BOOL_TYPE
			}else {
				if strings.Index(COLUMN_TYPE, "unsigned") >= 0 {
					ToDataType = dataType.UINT8_TYPE
				} else {
					ToDataType = dataType.INT8_TYPE
				}
			}
			break
		case "smallint":
			if strings.Index(COLUMN_TYPE,"unsigned") >= 0{
				ToDataType = dataType.UINT16_TYPE
			}else{
				ToDataType = dataType.INT16_TYPE
			}
			break
		case  "mediumint","int":
			if strings.Index(COLUMN_TYPE,"unsigned") >= 0{
				ToDataType = dataType.UINT32_TYPE
			}else{
				ToDataType = dataType.INT32_TYPE
			}
			break
		case "bigint":
			if strings.Index(COLUMN_TYPE,"unsigned") >= 0{
				ToDataType = dataType.UINT64_TYPE
			}else{
				ToDataType = dataType.INT64_TYPE
			}
			break

		case "float":
			ToDataType = dataType.FLOAT32_TYPE
			break
		case "double":
			ToDataType = dataType.FLOAT64_TYPE
			break
		case "decimal":
			ToDataType = dataType.STRING_TYPE
			break
		case "year":
			ToDataType = dataType.STRING_TYPE
			break
		case "bit":
			ToDataType = dataType.BIT_TYPE
			break
		case "bool":
			ToDataType = dataType.BOOL_TYPE
			break
		default:
			ToDataType = dataType.STRING_TYPE
			break
		}

		FieldList = append(FieldList,TableStruct{
			COLUMN_NAME:	COLUMN_NAME,
			COLUMN_DEFAULT:	COLUMN_DEFAULT,
			IS_NULLABLE:	IS_NULLABLE,
			COLUMN_TYPE:	COLUMN_TYPE,
			COLUMN_KEY:		COLUMN_KEY,
			EXTRA:			EXTRA,
			COLUMN_COMMENT:	COLUMN_COMMENT,
			DATA_TYPE:		DATA_TYPE,
			NUMERIC_PRECISION:NUMERIC_PRECISION,
			NUMERIC_SCALE:	NUMERIC_SCALE,
			ToDataType:		ToDataType,
		})
	}
	return FieldList
}