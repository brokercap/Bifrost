package history

import (
	"github.com/brokercap/Bifrost/server"
	"fmt"
	"log"
	"database/sql/driver"
)

func CheckWhere(dbName,SchemaName,TableName,Where string) error {
	if Where == ""{
		return nil
	}
	dbObj := server.GetDBObj(dbName)
	if dbObj == nil {
		return fmt.Errorf("%s not exist", dbName)
	}
	return CheckWhere0(dbObj.ConnectUri,SchemaName,TableName,Where)
}

func CheckWhere0(Uri string,SchemaName string,TableName string,Where string) error {
	db := DBConnect(Uri)
	defer db.Close()
	sql := "SELECT * FROM `" + SchemaName + "`.`" + TableName + "` WHERE "+ Where + " LIMIT 1"
	stmt, err := db.Prepare(sql)
	if err != nil{
		log.Println("CheckWhere:",err,"sql:",sql)
		return err
	}
	defer stmt.Close()
	rows, err := stmt.Query([]driver.Value{})
	if err != nil{
		return err
	}
	rows.Close()
	return nil
}