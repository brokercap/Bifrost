package serverdo

import (
	"github.com/hprose/hprose-golang/rpc"
	"log"
)

var i int

func init() {
	i = 1
}

func Check(context *rpc.HTTPContext) (e error) {
	log.Println("Check success")
	return nil
}

func Insert(SchemaName string, TableName string, data map[string]interface{}) (e error) {
	log.Println("Insert")
	log.Println("SchemaName:", SchemaName)
	log.Println("TableName:", TableName)
	log.Println(i, "data:", data)
	i++
	return nil
}

func Update(SchemaName string, TableName string, data []map[string]interface{}) (e error) {
	log.Println("Update")
	log.Println("SchemaName:", SchemaName)
	log.Println("TableName:", TableName)
	log.Println(i, "data:", data)
	i++
	return nil
}

func Delete(SchemaName string, TableName string, data map[string]interface{}) (e error) {
	log.Println("Delete")
	log.Println("SchemaName:", SchemaName)
	log.Println("TableName:", TableName)
	log.Println(i, "data:", data)
	i++
	return nil
}

func Query(SchemaName string, TableName string, data interface{}) (e error) {
	log.Println(i, "Query", "SchemaName:", SchemaName, "TableName:", TableName, "data:", data)
	i++
	return nil
}

func Commit(SchemaName string, TableName string, data interface{}) (e error) {
	log.Println(i, "Commit", "SchemaName:", SchemaName, "TableName:", TableName, "data:", data)
	i++
	return nil
}
