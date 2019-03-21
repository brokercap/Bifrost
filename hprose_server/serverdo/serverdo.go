package serverdo

import (
	"github.com/hprose/hprose-golang/rpc"
	"log"
)

func Check(context *rpc.HTTPContext) (e error) {
	log.Println("Check success")
	return nil
}

func Insert(SchemaName string,TableName string, data map[string]interface{}) (e error) {
	log.Println("Insert")
	log.Println("SchemaName:",SchemaName)
	log.Println("TableName:",TableName)
	log.Println("data:",data)
	return nil
}

func Update(SchemaName string,TableName string, data []map[string]interface{}) (e error){
	log.Println("Update")
	log.Println("SchemaName:",SchemaName)
	log.Println("TableName:",TableName)
	log.Println("data:",data)
	return nil
}

func Delete(SchemaName string,TableName string,data map[string]interface{}) (e error) {
	log.Println("Delete")
	log.Println("SchemaName:",SchemaName)
	log.Println("TableName:",TableName)
	log.Println("data:",data)
	return nil
}

func ToList(SchemaName string,TableName string,Type string,data interface{}) (e error) {
	log.Println("ToList")
	log.Println("Type:",Type)
	log.Println("SchemaName:",SchemaName)
	log.Println("TableName:",TableName)
	log.Println("data:",data)
	return nil
}