package main

import (
	"database/sql/driver"
	"flag"
	"fmt"
	"github.com/brokercap/Bifrost/Bristol/mysql"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

var DbConn MySQLConn

type MySQLConn struct {
	Uri string
	db  mysql.MysqlConnection
}

type MasterBinlogInfoStruct struct {
	File              string
	Position          int
	Binlog_Do_DB      string
	Binlog_Ignore_DB  string
	Executed_Gtid_Set string
}

func (This *MySQLConn) DBConnect() {
	This.db = mysql.NewConnect(This.Uri)
}

func (This *MySQLConn) GetBinLogInfo() MasterBinlogInfoStruct {
	sql := "SHOW MASTER STATUS"
	stmt, err := This.db.Prepare(sql)
	if err != nil {
		log.Println(err)
		return MasterBinlogInfoStruct{}
	}
	defer stmt.Close()
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	defer rows.Close()
	if err != nil {
		log.Printf("%v\n", err)
		return MasterBinlogInfoStruct{}
	}
	var File string
	var Position int
	var Binlog_Do_DB string
	var Binlog_Ignore_DB string
	var Executed_Gtid_Set string
	for {
		dest := make([]driver.Value, 4, 4)
		errs := rows.Next(dest)
		if errs != nil {
			return MasterBinlogInfoStruct{}
		}
		File = dest[0].(string)
		Binlog_Do_DB = dest[2].(string)
		Binlog_Ignore_DB = dest[3].(string)
		Executed_Gtid_Set = ""
		PositonString := fmt.Sprint(dest[1])
		Position, _ = strconv.Atoi(PositonString)
		break
	}

	return MasterBinlogInfoStruct{
		File:              File,
		Position:          Position,
		Binlog_Do_DB:      Binlog_Do_DB,
		Binlog_Ignore_DB:  Binlog_Ignore_DB,
		Executed_Gtid_Set: Executed_Gtid_Set,
	}

}

func (This *MySQLConn) GetServerId() int {
	sql := "show variables like 'server_id'"
	stmt, err := This.db.Prepare(sql)
	if err != nil {
		log.Println(err)
		return 0
	}
	defer stmt.Close()
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	if err != nil {
		log.Printf("%v\n", err)
		return 0
	}
	defer rows.Close()
	var ServerId int
	for {
		dest := make([]driver.Value, 2, 2)
		errs := rows.Next(dest)
		if errs != nil {
			return 0
		}
		ServerIdString := fmt.Sprint(dest[1])
		ServerId, _ = strconv.Atoi(ServerIdString)
		break
	}
	return ServerId
}

func (This *MySQLConn) ExecSQL(sql string) {
	p := make([]driver.Value, 0)
	_, err := This.db.Exec(sql, p)
	if err != nil {
		log.Println("ExecSQL:", sql, " Err:", err)
	} else {
		log.Println("ExecSQL success:", sql)
	}
	return
}

func GetSchemaTableFieldAndVal(db mysql.MysqlConnection, schema string, table string) (sqlstring string, data []driver.Value) {
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
		var defaultVal string
		EXTRA = fmt.Sprint(dest[3])

		if EXTRA == "auto_increment" {
			continue
		} else {
			fieldType := fmt.Sprint(dest[2])
			if dest[1] == nil {
				defaultVal = ""
			} else {
				defaultVal = fmt.Sprint(dest[1])
			}
			COLUMN_TYPE := fmt.Sprint(dest[4])
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

			fieldNAme = fmt.Sprint(dest[0])
			if sqlk == "" {
				sqlk = "`" + fieldNAme + "`"
				sqlv = "?"
			} else {
				sqlk += ",`" + fieldNAme + "`"
				sqlv += ",?"
			}
		}

	}
	sqlstring = "INSERT INTO " + schema + "." + table + " (" + sqlk + ") values (" + sqlv + ")"
	return
}

func forInsert(schema string, table string, count int) {
	sql, v := GetSchemaTableFieldAndVal(DbConn.db, schema, table)
	//return
	stmt, err := DbConn.db.Prepare(sql)
	if err != nil {
		log.Println("db Prepare err:", err)
		return
	}
	for i := 0; i < count; i++ {
		_, err2 := stmt.Exec(v)
		if err2 != nil {
			log.Println("db stmt err:", err2)
			break
		}
	}
}

var StartTime int64 = 0
var count *int

func producData(Schema, TableName string) {
	var sqlList = []string{
		"CREATE DATABASE /*!32312 IF NOT EXISTS*/ `" + Schema + "`",
		//"DROP TABLE IF EXISTS "+Schema+".`"+TableName+"`",
		"CREATE TABLE " + Schema + ".`" + TableName + "` (" +
			"`id` int(11) unsigned NOT NULL AUTO_INCREMENT," +
			"`testtinyint` tinyint(4) NOT NULL DEFAULT '-1'," +
			"`testsmallint` smallint(6) NOT NULL DEFAULT '-2'," +
			"`testmediumint` mediumint(8) NOT NULL DEFAULT '-3'," +
			"`testint` int(11) NOT NULL DEFAULT '-4'," +
			"`testbigint` bigint(20) NOT NULL DEFAULT '-5'," +
			"`testvarchar` varchar(10) NOT NULL," +
			"`testchar` char(2) NOT NULL," +
			"`testenum` enum('en1','en2','en3') NOT NULL DEFAULT 'en1'," +
			"`testset` set('set1','set2','set3') NOT NULL DEFAULT 'set1'," +
			"`testtime` time NOT NULL DEFAULT '00:00:00'," +
			"`testdate` date NOT NULL DEFAULT '0000-00-00'," +
			"`testyear` year(4) NOT NULL DEFAULT '1989'," +
			"`testtimestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
			"`testdatetime` datetime NOT NULL DEFAULT '0000-00-00 00:00:00'," +
			"`testfloat` float(9,2) NOT NULL DEFAULT '0.00'," +
			"`testdouble` double(9,2) NOT NULL DEFAULT '0.00'," +
			"`testdecimal` decimal(9,2) NOT NULL DEFAULT '0.00'," +
			"`testtext` text NOT NULL," +
			"`testblob` blob NOT NULL," +
			"`testbit` bit(8) NOT NULL DEFAULT b'0'," +
			"`testbool` tinyint(1) NOT NULL DEFAULT '0'," +
			"`testmediumblob` mediumblob NOT NULL," +
			"`testlongblob` longblob NOT NULL," +
			"`testtinyblob` tinyblob NOT NULL," +
			"`test_unsinged_tinyint` tinyint(4) unsigned NOT NULL DEFAULT '1'," +
			"`test_unsinged_smallint` smallint(6) unsigned NOT NULL DEFAULT '2'," +
			"`test_unsinged_mediumint` mediumint(8) unsigned NOT NULL DEFAULT '3'," +
			"`test_unsinged_int` int(11) unsigned NOT NULL DEFAULT '4'," +
			"`test_unsinged_bigint` bigint(20) unsigned NOT NULL DEFAULT '5'," +
			"PRIMARY KEY (`id`)" +
			") ENGINE=MyISAM AUTO_INCREMENT=0 DEFAULT CHARSET=utf8",
	}
	for _, sql := range sqlList {
		//log.Println("exec sql:",sql)
		DbConn.ExecSQL(sql)
	}
	log.Println("insert data start")
	StartTime = time.Now().Unix()
	forInsert(Schema, TableName, *count)
	EndTime := time.Now().Unix()
	log.Println("insert data over")
	log.Println("use time", EndTime-StartTime, "s")
}
func main() {
	host := flag.String("h", "127.0.0.1", "-h")
	user := flag.String("u", "root", "-u")
	pwd := flag.String("p", "", "-p")
	port := flag.String("P", "3306", "-P")
	conndb := flag.String("conndb", "test", "-conndb")
	table := flag.String("table", "bristol_performance_test", "-table")
	schema := flag.String("schema", "jc3wish_test", "-schema")
	count = flag.Int("count", 100000, "-count")
	onlydata := flag.String("onlydata", "false", "-onlydata")
	master_log_file := flag.String("master_log_file", "", "-master_log_file")
	master_log_pos := flag.Int("master_log_pos", 0, "-master_log_pos")
	flag.Parse()

	DataSource := *user + ":" + *pwd + "@tcp(" + *host + ":" + *port + ")/" + *conndb

	//DataSource := "root:root@tcp(127.0.0.1:3306)/test"
	Schema := *schema
	TableName := *table

	DbConn.Uri = DataSource
	DbConn.DBConnect()
	var filename string
	var position uint32
	DbConn.DBConnect()
	if *master_log_file == "" || *master_log_pos <= 0 {

		producData(Schema, TableName)

		BinlogInfo := DbConn.GetBinLogInfo()
		if BinlogInfo.File == "" {
			log.Println("not support binlod")
			os.Exit(1)
		}
		filename = BinlogInfo.File
		position = uint32(BinlogInfo.Position)

		if *onlydata == "true" {
			producData(Schema, TableName)
			log.Println("onlydata == true, no test Bristol")
			os.Exit(0)
		}
	} else {
		filename = *master_log_file
		position = uint32(*master_log_pos)
	}

	MastSeverId := DbConn.GetServerId()
	MyServerID := uint32(MastSeverId + 249)

	reslut := make(chan error, 1)
	BinlogDump := mysql.NewBinlogDump(
		DataSource,
		callback,
		[]mysql.EventType{
			//mysql.QUERY_EVENT,
			mysql.WRITE_ROWS_EVENTv1, mysql.UPDATE_ROWS_EVENTv1, mysql.DELETE_ROWS_EVENTv1,
			mysql.WRITE_ROWS_EVENTv0, mysql.UPDATE_ROWS_EVENTv0, mysql.DELETE_ROWS_EVENTv0,
			mysql.WRITE_ROWS_EVENTv2, mysql.UPDATE_ROWS_EVENTv2, mysql.DELETE_ROWS_EVENTv2,
		},
		nil,
		nil)
	BinlogDump.AddReplicateDoDb(Schema, TableName)
	log.Println("Schema:", Schema)
	log.Println("TableName:", TableName)
	log.Println("analysis binlog start")
	log.Println("start binlog info:", filename, position)
	StartTime = time.Now().Unix()
	go BinlogDump.StartDumpBinlog(filename, position, MyServerID, reslut, "", 0)
	go func() {
		for {
			v := <-reslut
			log.Printf("monitor reslut:%s \r\n", v)
		}
	}()
	for {
		time.Sleep(10 * time.Second)
	}
}

var insertCount int = 0

func callback(data *mysql.EventReslut) {
	insertCount++
	if insertCount == *count {
		overTime := time.Now().Unix()
		log.Println("analysis binlog over")
		log.Println("analysis success count:", insertCount)
		BinlogInfo := DbConn.GetBinLogInfo()
		log.Println("end binlog info:", BinlogInfo.File, BinlogInfo.Position)
		log.Println("use time:", overTime-StartTime, "s")
		os.Exit(0)
	}
	//log.Println(data)
}
