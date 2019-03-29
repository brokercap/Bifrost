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
package server

import (
	"fmt"
)

func AddTable(db string,schema string,tableName string,channelId int) error{
	if _,ok:=DbList[db];!ok{
		return fmt.Errorf(db+" not exsit")
	}
	DbList[db].AddReplicateDoDb(schema)
	if DbList[db].AddTable(schema,tableName,channelId,0) == true{
		return nil
	}
	return fmt.Errorf("unkown error")
}


func DelTable(db string,schema string,tableName string) error{
	if _,ok:=DbList[db];!ok{
		return fmt.Errorf(db+"not exsit")
	}
	DbList[db].DelTable(schema,tableName)
	return nil
}

func AddTableToServer(db string,schemaName string,tableName string,ToServerInfo ToServer) error{
	if _,ok:=DbList[db];!ok{
		return fmt.Errorf(db+"not exsit")
	}
	key := schemaName + "-" + tableName
	if _, ok := DbList[db].tableMap[key]; !ok {
		return fmt.Errorf(key+" not exsit")
	} else {
		DbList[db].AddTableToServer(schemaName,tableName,&ToServerInfo)
	}
	return nil
}