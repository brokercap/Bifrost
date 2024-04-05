package server

import (
	"github.com/brokercap/Bifrost/config"
	"os"
	"time"
)

var serverStartTime = time.Now()

func GetServerStartTime() time.Time {
	return serverStartTime
}

func setServerStartTime(t time.Time) {
	if t.IsZero() {
		t = GetServerStartTimeByConfigFile()
	}
	if !serverStartTime.IsZero() {
		if t.After(serverStartTime) {
			return
		}
	}
	serverStartTime = t
}

func GetServerStartTimeByConfigFile() time.Time {
	fInfo, err := os.Stat(config.BifrostConfigFile)
	if err != nil {
		return time.Now()
	}
	return fInfo.ModTime()
}
