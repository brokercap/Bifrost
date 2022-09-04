package main

import (
	"database/sql/driver"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/brokercap/Bifrost/Bristol/mysql"
)

func main() {
	var timeInterval int
	host := flag.String("host", "192.168.126.140", "-host")
	user := flag.String("user", "root", "-user")
	pwd := flag.String("pwd", "root", "-pwd")
	port := flag.String("port", "3306", "-port")
	table := flag.String("table", "test1", "-test1")
	schema := flag.String("schema", "bifrost_test", "-schema")
	count := flag.Int("count", 1000, "-count")
	batchsize := flag.Int("batchsize", 50, "-batchsize")
	conn := flag.Int("conn", 2, "-conn")
	flag.IntVar(&timeInterval, "time_interval", 0, "-time_interval") // 单位毫秒
	flag.Parse()
	//root:root@tcp(10.40.2.41:3306)/test
	dbstring := *user + ":" + *pwd + "@tcp(" + *host + ":" + *port + ")/" + *schema
	println(dbstring)
	println("table:", *table)
	println("count:", *count)
	log.Println("start ", time.Now().Format("2006-01-02 15:04:05"))

	n := (*count) / (*conn)
	ws := &sync.WaitGroup{}
	ws.Add(*conn)
	for i := 0; i < *conn; i++ {
		go func() {
			defer func() {
				ws.Done()
			}()
			forInsert(dbstring, *schema, *table, n, *batchsize, timeInterval)
		}()
	}
	ws.Wait()
	log.Println("end ", time.Now().Format("2006-01-02 15:04:05"))
}

func DBConnect(uri string) mysql.MysqlConnection {
	db := mysql.NewConnect(uri)
	return db
}

func GetSchemaTableFieldAndVal(db mysql.MysqlConnection, schema string, table string, n int) (sqlstring string, data []driver.Value) {
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
	var ii int = -1
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
			ii++
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
			case "json":
				m := make(map[string][]interface{}, 1)
				m["key1"] = make([]interface{}, 0)
				m["key1"] = append(m["key1"], 1)
				m["key1"] = append(m["key1"], "2")
				m["key1"] = append(m["key1"], nil)
				m["key1"] = append(m["key1"], true)
				c, _ := json.Marshal(m)
				data = append(data, string(c))
				break
			default:
				data = append(data, "0")
				break
			}

			fieldNAme = string(dest[0].(string))
			if sqlk == "" {
				sqlk = "`" + fieldNAme + "`"
				switch reflect.TypeOf(data[ii]).Kind() {
				case reflect.Map:
					b, _ := json.Marshal(data[ii])
					sqlv = "'" + string(b) + "'"
					break
				case reflect.Bool:
					if data[ii].(bool) == true {
						sqlv = "true"
					} else {
						sqlv = "false"
					}
				default:
					vtmp := fmt.Sprint(data[ii])
					vtmp = strings.Replace(vtmp, "'", "", -1)
					sqlv = "'" + vtmp + "'"
					break
				}

			} else {
				sqlk += ",`" + fieldNAme + "`"
				switch reflect.TypeOf(data[ii]).Kind() {
				case reflect.Map:
					b, _ := json.Marshal(data[ii])
					sqlv += "'" + string(b) + "'"
					break
				case reflect.Bool:
					if data[ii].(bool) == true {
						sqlv += ",true"
					} else {
						sqlv += ",false"
					}
				default:
					vtmp := fmt.Sprint(data[ii])
					vtmp = strings.Replace(vtmp, "'", "", -1)
					sqlv += ",'" + vtmp + "'"
					break
				}
			}
		}

	}
	sqlstring = "INSERT INTO " + table + " (" + sqlk + ") values (" + sqlv + ")"
	for i := 1; i < n; i++ {
		sqlstring += ",(" + sqlv + ")"
	}
	log.Println(sqlstring)
	log.Println(data)
	return
}

func forInsert(uri string, schema string, table string, count int, batchsize int, timeInterval int) {
	n := count / batchsize
	if n == 0 {
		n = 1
	}
	db := DBConnect(uri)
	if count-batchsize < 0 {
		batchsize = count
	}
	sql, _ := GetSchemaTableFieldAndVal(db, schema, table, batchsize)
	var stmt driver.Stmt
	var err error
	stmt, err = db.Prepare(sql)
	if err != nil {
		log.Fatal("db Prepare err:", err)
		return
	}
	for i := 0; i < n; i++ {
		_, err2 := stmt.Exec([]driver.Value{})
		if err2 != nil {
			log.Println("db stmt err:", err2)
			break
		}
		if timeInterval > 0 {
			time.Sleep(time.Duration(timeInterval) * time.Millisecond)
		}
	}
}
