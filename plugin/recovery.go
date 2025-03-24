package plugin

import (
	"encoding/json"
	pluginStorage "github.com/brokercap/Bifrost/plugin/storage"
	"log"
)

func Recovery(data *json.RawMessage) {
	var toData map[string]pluginStorage.ToServer
	errors := json.Unmarshal([]byte(*data), &toData)
	if errors != nil {
		log.Println("to server recovry error:", errors)
		return
	}
	for name, v := range toData {
		pluginStorage.SetToServerInfo(name,
			pluginStorage.ToServer{
				PluginName: v.PluginName,
				ConnUri:    v.ConnUri,
				Notes:      v.Notes,
				MaxConn:    v.MaxConn,
				MinConn:    v.MinConn,
			})
	}
}

func SaveToServerData() interface{} {
	return pluginStorage.SaveToServerData()
}
