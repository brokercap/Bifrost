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
	"github.com/jc3wish/Bifrost/Bristol/mysql"
	"database/sql/driver"
	"log"
)

func init(){

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
		DatabaseName = string(dest[0].([]byte))
		databaseList = append(databaseList,DatabaseName)
	}
	//log.Println(databaseList)
	return databaseList
}

func GetSchemaTableList(db mysql.MysqlConnection,schema string) []string{

	tableList := make([]string,0)
	sql := "SELECT TABLE_NAME FROM `information_schema`.`TABLES` WHERE TABLE_SCHEMA = ?"

	stmt,err := db.Prepare(sql)
	if err !=nil{
		log.Println(err)
		return tableList
	}
	p := make([]driver.Value, 0)
	p = append(p,schema)
	rows, err := stmt.Query(p)
	defer rows.Close()
	if err != nil {
		log.Printf("%v\n", err)
		return tableList
	}

	for {
		dest := make([]driver.Value, 1, 1)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		var tableName string
		tableName = string(dest[0].([]byte))
		tableList = append(tableList,tableName)
	}
	//log.Println(tableList)
	return tableList
}

func GetSchemaTableFieldList(db mysql.MysqlConnection,schema string,table string) []string{

	FieldList := make([]string,0)
	sql := "SELECT COLUMN_NAME FROM `information_schema`.`columns` WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "

	stmt,err := db.Prepare(sql)
	if err !=nil{
		log.Println(err)
		return FieldList
	}
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
		dest := make([]driver.Value, 1, 1)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		var fieldNAme string
		fieldNAme = string(dest[0].([]byte))
		FieldList = append(FieldList,fieldNAme)
	}
	//log.Println(FieldList)
	return FieldList
}