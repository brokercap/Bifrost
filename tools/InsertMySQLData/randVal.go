package main

import (
	"database/sql/driver"
	"github.com/brokercap/Bifrost/Bristol/mysql"
	"log"
	"strings"
	"time"
)

func GetSchemaTableFieldAndValByRand(db mysql.MysqlConnection, schema string, table string) (sqlstring string, data []driver.Value) {
	sql := "SELECT COLUMN_NAME,COLUMN_DEFAULT,DATA_TYPE,EXTRA,COLUMN_TYPE FROM `information_schema`.`COLUMNS` WHERE TABLE_SCHEMA = '" + schema + "' AND  table_name = '" + table + "'"
	data = make([]driver.Value, 0)
	stmt, err := db.Prepare(sql)
	if err != nil {
		log.Println(err)
		return "", make([]driver.Value, 0)
	}
	p := make([]driver.Value, 0)
	//p = append(p,schema)
	//p = append(p,table)
	rows, err := stmt.Query(p)
	defer rows.Close()
	if err != nil {
		log.Printf("%v\n", err)
		return "", make([]driver.Value, 0)
	}
	var sqlk, sqlv = "", ""
	for {
		dest := make([]driver.Value, 5, 5)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		var fieldNAme, EXTRA string
		if dest[3] == nil {
			EXTRA = ""
		} else {
			EXTRA = string(dest[3].(string))
		}
		if EXTRA == "auto_increment" {
			continue
		} else {
			var defaultVal string
			fieldType := string(dest[2].(string))
			if dest[1] == nil {
				defaultVal = ""
			} else {
				defaultVal = string(dest[1].(string))
			}
			COLUMN_TYPE := string(dest[4].(string))
			switch fieldType {
			case "int", "tinyint", "smallint", "mediumint", "bigint":
				var unsigned bool = false
				if strings.Contains(COLUMN_TYPE, "unsigned") {
					unsigned = true
				}
				//continue
				if COLUMN_TYPE == "tinyint(1)" {
					data = append(data, false)
				} else {
					b := ""
					switch fieldType {
					case "tinyint":
						b = "1"
						break
					case "smallint":
						b = "2"
						break
					case "mediumint":
						b = "3"
						break
					case "int":
						b = "4"
						break
					case "bigint":
						b = "5"
						break
					}
					if unsigned == false {
						b = "-" + b
					}
					data = append(data, b)
				}
				break
			case "char", "varchar":
				data = append(data, "c")
				break
			case "text", "tinytext", "mediumtext", "smalltext":
				data = append(data, fieldType)
				break
			case "blob", "tinyblob", "mediumblob", "smallblob", "longblob":
				data = append(data, fieldType)
				break
			case "year":
				data = append(data, time.Now().Format("2006"))
				break
			case "time":
				data = append(data, time.Now().Format("15:04:05"))
				break
			case "date":
				data = append(data, time.Now().Format("2006-01-02"))
				break
			case "datetime":
				data = append(data, time.Now().Format("2006-01-02 15:04:05"))
				break
			case "timestamp":
				data = append(data, time.Now().Format("2006-01-02 15:04:05"))
				break
			case "bit":
				//continue
				data = append(data, "8")
				break
			case "float", "double", "decimal":
				data = append(data, 9.22)
				break
			case "set":
				if defaultVal != "" {
					data = append(data, defaultVal)
				} else {
					d := strings.Replace(COLUMN_TYPE, "set(", "", -1)
					d = strings.Replace(d, ")", "", -1)
					d = strings.Replace(d, "'", "", -1)
					set_values := strings.Split(d, ",")
					data = append(data, set_values[0])
				}
				break
			case "enum":
				if defaultVal != "" {
					data = append(data, defaultVal)
				} else {
					d := strings.Replace(COLUMN_TYPE, "enum(", "", -1)
					d = strings.Replace(d, ")", "", -1)
					d = strings.Replace(d, "'", "", -1)
					enum_values := strings.Split(d, ",")
					data = append(data, enum_values[0])
				}
				break
			default:
				data = append(data, "0")
				break
			}

			fieldNAme = string(dest[0].(string))
			if sqlk == "" {
				sqlk = "`" + fieldNAme + "`"
				sqlv = "?"
			} else {
				sqlk += ",`" + fieldNAme + "`"
				sqlv += ",?"
			}
		}

	}
	sqlstring = "INSERT INTO " + table + " (" + sqlk + ") values (" + sqlv + ")"
	log.Println(sqlstring)
	log.Println(data)
	return
}
