package plugin

import (
	"encoding/json"
	"log"
)

func Recovery(data *json.RawMessage){
	var toData map[string]ToServer
	errors := json.Unmarshal([]byte(*data),&toData)
	if errors != nil{
		log.Println("to server recovry error:",errors)
		return
	}
	for name,v:=range toData{
		SetToServerInfo(name,
			ToServer{
				PluginName:v.PluginName,
				ConnUri:v.ConnUri,
				Notes:v.Notes,
				MaxConn:v.MaxConn,
			})
	}
}

func SaveToServerData() interface{}{
	return ToServerMap
}