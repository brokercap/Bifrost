/*
Copyright [2018] [jc3wish]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package mysql

import (
	"database/sql/driver"
	"fmt"
	"github.com/brokercap/Bifrost/Bristol/mysql"
	"log"
	"strconv"
	"strings"
)

func init() {

}

func DBConnect(uri string) mysql.MysqlConnection {
	db := mysql.NewConnect(uri)
	return db
}

type MasterBinlogInfoStruct struct {
	File              string
	Position          int
	Binlog_Do_DB      string
	Binlog_Ignore_DB  string
	Executed_Gtid_Set string
}

func GetBinLogInfo(db mysql.MysqlConnection) MasterBinlogInfoStruct {
	sql := "SHOW MASTER STATUS"
	p := make([]driver.Value, 0)
	rows, err := db.Query(sql, p)
	if err != nil {
		log.Printf("%v\n", err)
		return MasterBinlogInfoStruct{}
	}
	defer rows.Close()
	var File string
	var Position int
	var Binlog_Do_DB string
	var Binlog_Ignore_DB string
	var Executed_Gtid_Set string
	for {
		dest := make([]driver.Value, 5, 5)
		errs := rows.Next(dest)
		if errs != nil {
			return MasterBinlogInfoStruct{}
		}
		File = dest[0].(string)
		Binlog_Do_DB = dest[2].(string)
		Binlog_Ignore_DB = dest[3].(string)
		if dest[4] == nil {
			Executed_Gtid_Set = ""
		} else {
			Executed_Gtid_Set = dest[4].(string)
		}
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

func GetServerId(db mysql.MysqlConnection) int {
	variablesMap := GetVariables(db, "server_id")
	if _, ok := variablesMap["server_id"]; !ok {
		return 0
	}
	ServerId, _ := strconv.Atoi(variablesMap["server_id"])
	return ServerId
}

func GetVariables(db mysql.MysqlConnection, variablesValue string) (data map[string]string) {
	data = make(map[string]string, 0)
	sql := "show variables like '" + variablesValue + "'"
	p := make([]driver.Value, 0)
	rows, err := db.Query(sql, p)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}
	defer rows.Close()
	for {
		dest := make([]driver.Value, 2, 2)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		variableName := dest[0].(string)
		value := dest[1].(string)
		data[variableName] = value
	}
	return
}

// 获取当前用户授权语句
func GetGrantsFor(db mysql.MysqlConnection) (grantSQL string, err error) {
	sql := "SHOW GRANTS FOR CURRENT_USER()"
	p := make([]driver.Value, 0)
	rows, err := db.Query(sql, p)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}
	defer rows.Close()
	for {
		dest := make([]driver.Value, 1, 1)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		grantSQL = dest[0].(string)
		break
	}
	return
}

// 校验用户是否拥有权限
func CheckUserSlavePrivilege(db mysql.MysqlConnection) (err error) {
	var grantSQL string
	grantSQL, err = GetGrantsFor(db)
	if err != nil {
		return
	}
	if grantSQL == "" {
		return fmt.Errorf("请确认当前帐号是否能正确连接！")
	}
	if strings.Index(grantSQL, "ALL PRIVILEGES") > 0 {
		return nil
	}
	errArr := make([]string, 0)
	if strings.Index(grantSQL, "SELECT") < 0 {
		errArr = append(errArr, "SELECT")
	}
	if strings.Index(grantSQL, "SHOW DATABASES") < 0 {
		errArr = append(errArr, "SHOW DATABASES")
	}
	if strings.Index(grantSQL, "SUPER") < 0 {
		errArr = append(errArr, "SUPER")
	}

	if strings.Index(grantSQL, "REPLICATION SLAVE") < 0 {
		errArr = append(errArr, "REPLICATION SLAVE")
	}

	if strings.Index(grantSQL, "EVENT") < 0 {
		errArr = append(errArr, "EVENT")
	}

	if len(errArr) > 0 {
		err = fmt.Errorf("MySQL权限不足，没有权限: %s", strings.Replace(fmt.Sprint(errArr), " ", ",", -1))
	}
	return
}

func GetMySQLVersion(db mysql.MysqlConnection) string {
	sql := "SELECT version()"
	p := make([]driver.Value, 0)
	rows, err := db.Query(sql, p)
	if err != nil {
		log.Printf("sql:%s, err:%v\n", sql, err)
		return ""
	}
	defer rows.Close()
	var version string
	for {
		dest := make([]driver.Value, 1, 1)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		version = dest[0].(string)
		break
	}
	return version
}
