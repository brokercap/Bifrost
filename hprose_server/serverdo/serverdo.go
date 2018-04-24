package serverdo

import (
	"github.com/hprose/hprose-golang/rpc"
	dataDriver "database/sql/driver"
	"log"
)

func Check(context *rpc.HTTPContext) (e error) {
	log.Println("Check success")
	return nil
}

func Insert(key string,timeout int, data map[string]dataDriver.Value) (e error) {
	log.Println("Insert")
	log.Println("key:",key)
	log.Println("timeout:",timeout)
	log.Println("data:",data)
	return nil
}

func Update(key string,timeout int, data map[string]dataDriver.Value) (e error){
	log.Println("Update")
	log.Println("key:",key)
	log.Println("timeout:",timeout)
	log.Println("data:",data)
	return nil
}

func Delete(key string) (e error) {
	log.Println("Delete")
	log.Println("key:",key)
	return nil
}

func ToList(key string,timeout int, data map[string]dataDriver.Value) (e error) {
	log.Println("ToList")
	log.Println("key:",key)
	log.Println("timeout:",timeout)
	log.Println("data:",data)
	return nil
}