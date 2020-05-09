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
package manager

import (
	"github.com/brokercap/Bifrost/Bristol/mysql"
	"database/sql/driver"
	"log"
	"strconv"
	"fmt"
	"strings"
)

func init(){

}

func tansferSchemaName(schemaName string) string  {
	if schemaName == "AllDataBases"{
		return "*"
	}
	return schemaName
}

func tansferTableName(tableName string) string  {
	if tableName == "AllTables"{
		return "*"
	}
	return tableName
}

func DBConnect(uri string) mysql.MysqlConnection{
	db := mysql.NewConnect(uri)
	return db
}

func GetSchemaList(db mysql.MysqlConnection) []string{
	databaseList := make([]string,0)
	sql := "select `SCHEMA_NAME` from `information_schema`.`SCHEMATA`"

	stmt,err := db.Prepare(sql)
	if err !=nil{
		log.Println(err)
		return databaseList
	}
	defer stmt.Close()
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	defer rows.Close()
	if err != nil {
		log.Printf("%v\n", err)
		return databaseList
	}

	for {
		dest := make([]driver.Value, 1, 1)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		var DatabaseName string
		DatabaseName = dest[0].(string)
		databaseList = append(databaseList,DatabaseName)
	}
	//log.Println(databaseList)
	return databaseList
}

type TableListStruct struct {
	TableName string
	TableType string
}

func GetSchemaTableList(db mysql.MysqlConnection,schema string) []TableListStruct{

	tableList := make([]TableListStruct,0)
	sql := "SELECT TABLE_NAME,TABLE_TYPE FROM `information_schema`.`TABLES` WHERE TABLE_SCHEMA = ?"

	stmt,err := db.Prepare(sql)
	if err !=nil{
		log.Println(err)
		return tableList
	}
	defer stmt.Close()
	p := make([]driver.Value, 0)
	p = append(p,schema)
	rows, err := stmt.Query(p)
	defer rows.Close()
	if err != nil {
		log.Printf("%v\n", err)
		return tableList
	}

	for {
		dest := make([]driver.Value, 2, 2)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		var tableName string
		var tableType string
		tableName = dest[0].(string)
		tableType = dest[1].(string)
		tableList = append(tableList,TableListStruct{TableName:tableName,TableType:tableType})
	}
	//log.Println(tableList)
	return tableList
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
	sql := "SELECT `COLUMN_NAME`,`COLUMN_DEFAULT`,`IS_NULLABLE`,`COLUMN_TYPE`,`COLUMN_KEY`,`EXTRA`,`COLUMN_COMMENT`,`DATA_TYPE`,`NUMERIC_PRECISION`,`NUMERIC_SCALE` FROM `information_schema`.`columns` WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "

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
			t := dest[1].(string)
			COLUMN_DEFAULT 	= &t
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
			t := dest[8].(uint64)
			NUMERIC_PRECISION 	= &t
		}
		if dest[9] == nil{
			NUMERIC_SCALE 	= nil
		}else{
			t := dest[9].(uint64)
			NUMERIC_SCALE 	= &t
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
			NUMERIC_PRECISION: NUMERIC_PRECISION,
			NUMERIC_SCALE:	NUMERIC_SCALE,
		})
	}
	return FieldList
}

type MasterBinlogInfoStruct struct {
	File string
	Position int
	Binlog_Do_DB string
	Binlog_Ignore_DB string
	Executed_Gtid_Set string
}

func GetBinLogInfo(db mysql.MysqlConnection) MasterBinlogInfoStruct{
	sql := "SHOW MASTER STATUS"
	stmt,err := db.Prepare(sql)
	if err !=nil{
		log.Println(err)
		return MasterBinlogInfoStruct{}
	}
	defer stmt.Close()
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
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
		Position,_ = strconv.Atoi(PositonString)
		break
	}

	return MasterBinlogInfoStruct{
		File:File,
		Position:Position,
		Binlog_Do_DB:Binlog_Do_DB,
		Binlog_Ignore_DB:Binlog_Ignore_DB,
		Executed_Gtid_Set:Executed_Gtid_Set,
	}
}

func GetServerId(db mysql.MysqlConnection) int{
	variablesMap := GetVariables(db,"server_id")
	if _,ok := variablesMap["server_id"];!ok{
		return 0
	}
	ServerId,_ := strconv.Atoi(variablesMap["server_id"])
	return ServerId
}

func GetVariables(db mysql.MysqlConnection,variablesValue string) (data map[string]string){
	data = make(map[string]string,0)
	sql := "show variables like '"+variablesValue+"'"
	stmt,err := db.Prepare(sql)
	if err !=nil{
		log.Println(err)
		return
	}
	defer stmt.Close()
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}
	defer rows.Close()
	for{
		dest := make([]driver.Value, 2, 2)
		err := rows.Next(dest)
		if err != nil{
			break
		}
		variableName := dest[0].(string)
		value := dest[1].(string)
		data[variableName] = value
	}
	return
}

//获取当前用户授权语句
func GetGrantsFor(db mysql.MysqlConnection) (grantSQL string,err error){
	sql := "SHOW GRANTS FOR CURRENT_USER()"
	stmt,err := db.Prepare(sql)
	if err !=nil{
		log.Println(err)
		return
	}
	defer stmt.Close()
	p := make([]driver.Value, 0)
	rows, err := stmt.Query(p)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}
	defer rows.Close()
	for{
		dest := make([]driver.Value, 1, 1)
		err := rows.Next(dest)
		if err != nil{
			break
		}
		grantSQL = dest[0].(string)
		break
	}
	return
}

// 校验用户是否拥有权限
func CheckUserSlavePrivilege(db mysql.MysqlConnection) (err error){
	var grantSQL string
	grantSQL,err = GetGrantsFor(db)
	if err != nil {
		return
	}
	if grantSQL == ""{
		return fmt.Errorf("请确认当前帐号是否能正确连接！")
	}
	if strings.Index(grantSQL,"ALL PRIVILEGES") > 0{
		return nil
	}
	errArr := make([]string,0)
	if strings.Index(grantSQL,"SELECT") < 0{
		errArr = append(errArr,"SELECT")
	}
	if strings.Index(grantSQL,"SHOW DATABASES") < 0{
		errArr = append(errArr,"SHOW DATABASES")
	}
	if strings.Index(grantSQL,"SUPER") < 0{
		errArr = append(errArr,"SUPER")
	}

	if strings.Index(grantSQL,"REPLICATION SLAVE") < 0{
		errArr = append(errArr,"REPLICATION SLAVE")
	}

	if strings.Index(grantSQL,"EVENT") < 0{
		errArr = append(errArr,"EVENT")
	}

	if len(errArr) > 0{
		err = fmt.Errorf("MySQL权限不足，没有权限: %s",strings.Replace(fmt.Sprint(errArr)," ", ",", -1))
	}
	return
}