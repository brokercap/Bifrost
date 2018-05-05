package main

import (
	"flag"
	"log"
	"database/sql/driver"
	"github.com/Bifrost/Bristol/mysql"
	"time"
	"strings"
)

func main(){
	host := flag.String("host", "127.0.0.1", "-host")
	user := flag.String("user", "root", "-user")
	pwd := flag.String("pwd", "root", "-pwd")
	port := flag.String("port", "3306", "-port")
	table := flag.String("table", "test1", "-test1")
	schema := flag.String("schema", "bifrost_test", "-schema")
	count := flag.Int("count", 100000, "-count")
	flag.Parse()
	//root:root@tcp(10.40.2.41:3306)/test
	dbstring := *user+":"+*pwd+"@tcp("+*host+":"+*port+")/"+*schema
	println(dbstring)
	println("table:",*table)
	println("count:",*count)
	log.Println("start ",time.Now().Format("2006-01-02 15:04:05"))
	forInsert(dbstring,*schema,*table,*count)
	log.Println("end ",time.Now().Format("2006-01-02 15:04:05"))
}

func DBConnect(uri string) mysql.MysqlConnection{
	db := mysql.NewConnect(uri)
	return db
}

func GetSchemaTableFieldAndVal(db mysql.MysqlConnection,schema string,table string) (sqlstring string, data []driver.Value){
	sql := "SELECT COLUMN_NAME,COLUMN_DEFAULT,DATA_TYPE,EXTRA,COLUMN_TYPE FROM `information_schema`.`COLUMNS` WHERE TABLE_SCHEMA = '"+schema+"' AND  table_name = '"+table+"'"
	data = make([]driver.Value,0)
	stmt,err := db.Prepare(sql)
	if err !=nil{
		log.Println(err)
		return "", make([]driver.Value,0)
	}
	p := make([]driver.Value, 0)
	//p = append(p,schema)
	//p = append(p,table)
	rows, err := stmt.Query(p)
	defer rows.Close()
	if err != nil {
		log.Printf("%v\n", err)
		return "", make([]driver.Value,0)
	}
	var sqlk ,sqlv = "",""
	for {
		dest := make([]driver.Value, 5, 5)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		var fieldNAme, EXTRA string
		EXTRA = string(dest[3].([]byte))

		if EXTRA == "auto_increment" {
			continue
		} else {
			fieldType := string(dest[2].([]byte))
			defaultVal := string(dest[1].([]byte))
			COLUMN_TYPE := string(dest[4].([]byte))
			switch fieldType {
			case "int", "tinyint", "smallint", "mediumint", "bigint":
				//continue
				if COLUMN_TYPE == "tinyint(1)"{
					data = append(data,false)
				}else{
					data = append(data,"11")
				}
				break
			case "char","varchar":
				data = append(data,"c")
				break
			case "text","tinytext","mediumtext","smalltext":
				data = append(data,"texttexttexttexttexttexttext")
				break
			case "blob","tinyblob","mediumblob","smallblob","longblob":
				data = append(data,"blobblobblobblobblobblobblobblob")
				break
			case "year":
				data = append(data,time.Now().Format("2006"))
				break
			case "time":
				data = append(data,time.Now().Format("15:04:05"))
				break
			case "date":
				data = append(data,time.Now().Format("2006-01-02"))
				break
			case "datetime":
				data = append(data,time.Now().Format("2006-01-02 15:04:05"))
				break
			case "timestamp":
				data = append(data,time.Now().Format("2006-01-02 15:04:05"))
				break
			case "bit":
				//continue
				data = append(data,"8")
				break
			case "float","double","decimal":
				data = append(data,9.22)
				break
			case "set":
				if defaultVal != "" {
					data = append(data,defaultVal)
				}else{
					d := strings.Replace(COLUMN_TYPE, "set(", "", -1)
					d = strings.Replace(d, ")", "", -1)
					d = strings.Replace(d, "'", "", -1)
					set_values := strings.Split(d, ",")
					data = append(data,set_values[0])
				}
				break
			case "enum":
				if defaultVal != "" {
					data = append(data,defaultVal)
				}else{
					d := strings.Replace(COLUMN_TYPE, "enum(", "", -1)
					d = strings.Replace(d, ")", "", -1)
					d = strings.Replace(d, "'", "", -1)
					enum_values := strings.Split(d, ",")
					data = append(data,enum_values[0])
				}
				break
			default:
				data = append(data,"0")
				break
			}

			fieldNAme = string(dest[0].([]byte))
			if sqlk == "" {
				sqlk = "`" + fieldNAme + "`"
				sqlv = "?"
			} else {
				sqlk += ",`" + fieldNAme + "`"
				sqlv += ",?"
			}
		}

	}
	sqlstring = "INSERT INTO "+table+" ("+sqlk+") values ("+sqlv+")"
	log.Println(sqlstring)
	log.Println(data)
	return
}

func forInsert(uri string,schema string,table string,count int){
	db := DBConnect(uri)
	sql,v := GetSchemaTableFieldAndVal(db,schema,table)
	//return
	stmt,err := db.Prepare(sql)
	if err != nil{
		log.Println("db Prepare err:",err)
		return
	}
	for i:=0;i<count;i++{
		_, err2 := stmt.Exec(v)
		if err2 != nil {
			log.Println("db stmt err:", err2)
			break
		}
	}
}