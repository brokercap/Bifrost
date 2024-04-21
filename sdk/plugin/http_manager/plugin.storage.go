package http_manager

import "github.com/brokercap/Bifrost/plugin/storage"

func SetToServer(toServerKey string, pluginName string, Uri string) bool {
	storage.SetToServerInfo(toServerKey, storage.ToServer{PluginName: pluginName, ConnUri: Uri, Notes: ""})
	return true
}
