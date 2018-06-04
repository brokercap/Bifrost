package main

import "github.com/jc3wish/Bifrost/toserver"
import "github.com/jc3wish/Bifrost/server"

func main(){
	doStart()
}

func doStart() {
	toserver.SetToServerInfo("redisTest", "redis", "127.0.0.1:6379", "redis test")

	filename := "mysql-bin.000022"
	var position uint32 = 13333
	db := server.AddNewDB("TestDBName", "root:root@tcp(127.0.0.1:3306)/test", filename, position, 3,"",0)
	if db == nil{
		return
	}
	m := make([]string, 0)
	m = append(m, "testdbcreate")
	db.SetReplicateDoDb(m)

	ch := db.AddChannel("default",1)
	ch.Start()

	db.AddTable("testdbcreate", "testdb", 1)
	db.AddTableToServer("testdbcreate", "testdb",
		server.ToServer{
			MustBeSuccess: true,
			Type:          "set",
			DataType:	   "string",
			KeyConfig:     "{$TableName}",
			ValueConfig:   "{id:{$id},\"aaa\":{$aaa}}",
			ToServerKey:   "redisTest",
		})

	db.Start()
}

